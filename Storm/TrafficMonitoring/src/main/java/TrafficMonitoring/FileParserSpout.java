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
import org.slf4j.Logger;
import Util.MetricGroup;
import java.util.Scanner;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import Util.config.Configuration;
import org.apache.storm.utils.Utils;
import java.io.FileNotFoundException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import Constants.TrafficMonitoringConstants;
import org.apache.storm.task.TopologyContext;
import Constants.TrafficMonitoringConstants.*;
import com.google.common.collect.ImmutableMap;
import Constants.TrafficMonitoringConstants.Field;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

/** 
 *  @author  Alessandra Fais
 *  @version June 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  vehicle-traces, parsing it and generating the stream of records
 *  toward the MapMatchingBolt.
 */ 
public class FileParserSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(FileParserSpout.class);
    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    private Integer rate;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long epoch;
    private long runTimeSec;
    private boolean waitForShutdown;
    private long lastTupleTs;
    private Sampler throughput;
    private String city;
    // state of the spout (contains parsed data)
    private ArrayList<String> vehicles;
    private ArrayList<Double> latitudes;
    private ArrayList<Double> longitudes;
    private ArrayList<Double> speeds;
    private ArrayList<Integer> bearings;

    /**
     * Constructor: it expects the city, the generation rate and the parallelism degree.
     * @param c city to monitor
     * @param gen_rate the spout generates tuples at the rate given by this parameter
     * @param p_deg source parallelism degree
     */
    FileParserSpout(String c, int gen_rate, int p_deg, long _runTimeSec) {
        city = c;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        nt_execution = 0;       // number of generations of the dataset
        index = 0;
        epoch = 0;
        runTimeSec = _runTimeSec;
        waitForShutdown = false;
        lastTupleTs = 0;
        vehicles = new ArrayList<>();
        latitudes = new ArrayList<>();
        longitudes = new ArrayList<>();
        speeds = new ArrayList<>();
        bearings = new ArrayList<>();
    }

    // open method
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;
        // set city trace file path
        String city_tracefile =  config.getString(Conf.SPOUT_BEIJING);
        // save tuples as a state
        parse(city_tracefile);
        throughput = new Sampler();
    }

    // nextTuple method
    @Override
    public void nextTuple() {
        // check termination
        if (waitForShutdown) {
            return;
        }
        else if (epoch > 0 && (System.nanoTime() - epoch) / 1e9 > runTimeSec) {
            try {
                LOG.info("Killing the topology");
                // initiate topology shutdown from inside
                NimbusClient nimbusClient = NimbusClient.getConfiguredClient(Utils.readStormConfig());
                nimbusClient.getClient().killTopology(TrafficMonitoringConstants.DEFAULT_TOPO_NAME);
                waitForShutdown = true;
                return;
            }
            catch (TException e) {
                collector.reportError(e);
            }
        }
        // generate next tuple
        long timestamp = System.nanoTime();
        collector.emit(new Values(vehicles.get(index), latitudes.get(index), longitudes.get(index), speeds.get(index), bearings.get(index), timestamp));
        generated++;
        index++;
        lastTupleTs = timestamp;
        if (rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / rate) * 1e9);
            active_delay(delay_nsec);
        }
        // check the dataset boundaries
        if (index >= vehicles.size()) {
            index = 0;
            nt_execution++;
        }
        // set the starting time
        if (generated == 1) {
            epoch = System.nanoTime();
        }
    }

    // close method
    @Override
    public void close() {
        double rate = generated / ((lastTupleTs - epoch) / 1e9); // per second
        long t_elapsed = (long) ((lastTupleTs - epoch) / 1e6);  // elapsed time in milliseconds
        /*LOG.info("[Source] execution time: " + t_elapsed +
                " ms, generations: " + nt_execution +
                ", generated: " + generated +
                ", bandwidth: " + generated / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    // declareOutputFields method
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(Field.VEHICLE_ID, Field.LATITUDE, Field.LONGITUDE,
                        Field.SPEED, Field.BEARING, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------//

    /**
     * Parse vehicles' traces and populate the state of the spout.
     * @param city_tracefile dataset containing vehicles' traces for the given city
     */
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
}
