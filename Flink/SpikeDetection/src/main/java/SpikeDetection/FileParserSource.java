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

package SpikeDetection;

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
import Constants.SpikeDetectionConstants.Conf;
import Constants.SpikeDetectionConstants.Field;
import org.apache.flink.configuration.Configuration;
import Constants.SpikeDetectionConstants.DatasetParsing;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  measurements from a set of sensor devices, parsing it
 *  and generating the stream of records toward the MovingAverageCalculator.
 *  
 *  Format of the input file:
 *  <date:yyyy-mm-dd, time:hh:mm:ss.xxx, epoch:int, deviceID:int, temperature:real, humidity:real, light:real, voltage:real>
 *  
 *  Data example can be found here: http://db.csail.mit.edu/labdata/labdata.html
 */ 
public class FileParserSource extends RichParallelSourceFunction<Source_Event> {
    private static final Logger LOG = Log.get(FileParserSource.class);
    private String file_path;
    private Integer rate;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long runTimeSec;
    private Sampler throughput;
    private boolean isRunning;
    // maps the property that the user wants to monitor (value from sd.properties:sd.parser.value_field)
    // to the corresponding field index
    private static final ImmutableMap<String, Integer> field_list = ImmutableMap.<String, Integer>builder()
            .put("temp", DatasetParsing.TEMP_FIELD)
            .put("humid", DatasetParsing.HUMID_FIELD)
            .put("light", DatasetParsing.LIGHT_FIELD)
            .put("volt", DatasetParsing.VOLT_FIELD)
            .build();
    private String value_field;
    private int value_field_key;
    // state of the spout (contains parsed data)
    private ArrayList<String> date;
    private ArrayList<String> time;
    private ArrayList<Integer> epoc;
    private ArrayList<String> devices;
    private ArrayList<Double> temperature;
    private ArrayList<Double> humidity;
    private ArrayList<Double> light;
    private ArrayList<Double> voltage;
    private ArrayList<Double> data;
    private Configuration config;

    // Constructor
    public FileParserSource(String file, int gen_rate, int p_deg, Configuration _config, long _runTimeSec) {
        file_path = file;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        nt_execution = 0;       // number of executions of nextTuple() method
        index = 0;
        runTimeSec = _runTimeSec;
        isRunning = true;
        date = new ArrayList<>();
        time = new ArrayList<>();
        epoc = new ArrayList<>();
        devices = new ArrayList<>();
        temperature = new ArrayList<>();
        humidity = new ArrayList<>();
        light = new ArrayList<>();
        voltage = new ArrayList<>();
        config = _config;
        // set value field to be monitored
        value_field = config.getString(Conf.PARSER_VALUE_FIELD, "temp");
        value_field_key = field_list.get(value_field);
        // save tuples as a state
        parseDataset();
        // data to be emitted (select w.r.t. value_field value)
        if (value_field_key == DatasetParsing.TEMP_FIELD)
            data = temperature;
        else if (value_field_key == DatasetParsing.HUMID_FIELD)
            data = humidity;
        else if (value_field_key == DatasetParsing.LIGHT_FIELD)
            data = light;
        else
            data = voltage;
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
            ctx.collect(new Source_Event(devices.get(index), data.get(index), timestamp));
            generated++;
            index++;
            if (rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            // check the dataset boundaries
            if (index >= devices.size()) {
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

    // parseDataset method
    private void parseDataset() {
        try {
            Scanner scan = new Scanner(new File(file_path));
            while (scan.hasNextLine()) {
                String[] fields = scan.nextLine().split("\\s+"); // regex quantifier (matches one or many whitespaces)
                //String date_str = String.format("%s %s", fields[DATE_FIELD], fields[TIME_FIELD]);
                if (fields.length >= 8) {
                    date.add(fields[DatasetParsing.DATE_FIELD]);
                    time.add(fields[DatasetParsing.TIME_FIELD]);
                    epoc.add(new Integer(fields[DatasetParsing.EPOCH_FIELD]));
                    devices.add(fields[DatasetParsing.DEVICEID_FIELD]);
                    temperature.add(new Double(fields[DatasetParsing.TEMP_FIELD]));
                    humidity.add(new Double(fields[DatasetParsing.HUMID_FIELD]));
                    light.add(new Double(fields[DatasetParsing.LIGHT_FIELD]));
                    voltage.add(new Double(fields[DatasetParsing.VOLT_FIELD]));
                    LOG.debug("[Source] tuple: deviceID " + fields[DatasetParsing.DEVICEID_FIELD] +
                            ", property " + value_field + " " + fields[value_field_key]);
                    LOG.debug("[Source] fields: " +
                                    fields[DatasetParsing.DATE_FIELD] + " " +
                            fields[DatasetParsing.TIME_FIELD] + " " +
                            fields[DatasetParsing.EPOCH_FIELD] + " " +
                            fields[DatasetParsing.DEVICEID_FIELD] + " " +
                            fields[DatasetParsing.TEMP_FIELD] + " " +
                            fields[DatasetParsing.HUMID_FIELD] + " " +
                            fields[DatasetParsing.LIGHT_FIELD] + " " +
                            fields[DatasetParsing.VOLT_FIELD]);
                }
                else
                    LOG.debug("[Source] incomplete record");
            }
            scan.close();
        }
        catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", file_path);
            throw new RuntimeException("The file '" + file_path + "' does not exists");
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
