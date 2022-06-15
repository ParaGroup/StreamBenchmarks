/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package TrafficMonitoring;

import Util.Log;
import Util.Sampler;
import java.io.File;
import java.util.Map;
import Util.MetricGroup;
import org.slf4j.Logger;
import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import Util.ThroughputCounter;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import static Constants.BaseConstants.*;
import com.google.common.collect.ImmutableMap;
import Constants.TrafficMonitoringConstants.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  vehicle-traces, parsing it and generating the stream of records
 *  toward the MapMatchingCalculator.
 */ 
public class FileParserSource extends RichParallelSourceFunction<Source_Event> {
    private static final Logger LOG = Log.get(FileParserSource.class);
    private Integer rate;
    private String city;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long runTimeSec;
    private Sampler throughput;
    private boolean isRunning;
    // state of the spout (contains parsed data)
    private ArrayList<String> vehicles;
    private ArrayList<Double> latitudes;
    private ArrayList<Double> longitudes;
    private ArrayList<Double> speeds;
    private ArrayList<Integer> bearings;
    private Configuration config;

    // Constructor
    public FileParserSource(String c, int gen_rate, int p_deg, Configuration _config, long _runTimeSec) {
        city = c;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        index = 0;
        nt_execution = 0;       // number of executions of nextTuple() method
        runTimeSec = _runTimeSec;
        isRunning = true;
        vehicles = new ArrayList<>();
        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        speeds = new ArrayList<>();
        bearings = new ArrayList<>();
        config = _config;
        // set city trace file path
        String city_tracefile =  config.getString(Conf.SPOUT_BEIJING, "tm.spout.beijing");
        // save tuples as a state
        parse(city_tracefile);
    }

    // open method
    @Override
    public void open(Configuration parameters) throws IOException {
        throughput = new Sampler();
    }

    // run method
    @Override
    public void run(SourceContext<Source_Event> ctx) throws Exception {
        long epoch = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - epoch < runTimeSec * 1e9) && isRunning) {
            // send tuple
            long timestamp = System.nanoTime();
            ctx.collect(new Source_Event(vehicles.get(index), latitudes.get(index), longitudes.get(index),
                        speeds.get(index), bearings.get(index), timestamp));
            generated++;
            index++;
            if (rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            // check the dataset boundaries
            if (index >= vehicles.size()) {
                index = 0;
                nt_execution++;
            }
        }
        // terminate the generation
        isRunning = false;
        // dump metric
        double rate = generated / ((System.nanoTime() - epoch) / 1e9); // per second
        long t_elapsed = (long) ((System.nanoTime() - epoch) / 1e6);  // elapsed time in milliseconds
        /*LOG.info("[Source] execution time: " + t_elapsed +
                 " ms, generations: " + nt_execution +
                 ", generated: " + generated +
                 ", bandwidth: " + rate +  // tuples per second
                 " tuples/s");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        ThroughputCounter.add(generated);
    }

    // parse method
    private void parse(String city_tracefile) {
        try {
            Scanner scan = new Scanner(new File(city_tracefile));
            while (scan.hasNextLine()) {
                String[] fields = scan.nextLine().split(",");
                /* 
                 * Beijing vehicle-trace dataset is used freely, with the following acknowledgement:
                 * “This code was obtained from research conducted by the University of Southern
                 * California’s Autonomous Networks Research Group, http://anrg.usc.edu“.
                 * 
                 * Format of the dataset:
                 * vehicleID, date-time, latitude, longitude, speed, bearing (direction)
                 */
                if (city.equals(City.BEIJING) && fields.length >= 7) {
                    vehicles.add(fields[BeijingParsing.B_VEHICLE_ID_FIELD]);
                    latitudes.add(Double.valueOf(fields[BeijingParsing.B_LATITUDE_FIELD]));
                    longitudes.add(Double.valueOf(fields[BeijingParsing.B_LONGITUDE_FIELD]));
                    speeds.add(Double.valueOf(fields[BeijingParsing.B_SPEED_FIELD]));
                    bearings.add(Integer.valueOf(fields[BeijingParsing.B_DIRECTION_FIELD]));
                    LOG.debug("[Source] Beijing Fields: {} ; {} ; {} ; {} ; {}",
                            fields[BeijingParsing.B_VEHICLE_ID_FIELD],
                            fields[BeijingParsing.B_LATITUDE_FIELD],
                            fields[BeijingParsing.B_LONGITUDE_FIELD],
                            fields[BeijingParsing.B_SPEED_FIELD],
                            fields[BeijingParsing.B_DIRECTION_FIELD]);
                }
            }
            scan.close();
        }
        catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", city_tracefile);
            throw new RuntimeException("The file '" + city_tracefile + "' does not exists");
        }
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }

    @Override
    public void cancel() {
        //LOG.info("cancel");
        isRunning = false;
    }

    @Override
    public void close() {
        //LOG.info("close");
    }
}
