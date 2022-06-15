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
import Constants.SpikeDetectionConstants;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import Constants.SpikeDetectionConstants.*;
import org.apache.storm.task.TopologyContext;
import com.google.common.collect.ImmutableMap;
import Constants.SpikeDetectionConstants.Field;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

/** 
 *  @author  Alessandra Fais
 *  @version May 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  measurements from a set of sensor devices, parsing it
 *  and generating the stream of records toward the MovingAverageBolt.
 *  
 *  Format of the input file:
 *  <date:yyyy-mm-dd, time:hh:mm:ss.xxx, epoch:int, deviceID:int, temperature:real, humidity:real, light:real, voltage:real>
 *  
 *  Data example can be found here: http://db.csail.mit.edu/labdata/labdata.html
 */ 
public class FileParserSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(FileParserSpout.class);
    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;

    // maps the property that the user wants to monitor (value from sd.properties:sd.parser.value_field)
    // to the corresponding field index
    private static final ImmutableMap<String, Integer> field_list = ImmutableMap.<String, Integer>builder()
            .put("temp", DatasetParsing.TEMP_FIELD)
            .put("humid", DatasetParsing.HUMID_FIELD)
            .put("light", DatasetParsing.LIGHT_FIELD)
            .put("volt", DatasetParsing.VOLT_FIELD)
            .build();

    private String file_path;
    private Integer rate;
    private String value_field;
    private int value_field_key;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long epoch;
    private long runTimeSec;
    private boolean waitForShutdown;
    private long lastTupleTs;
    private Sampler throughput;
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

    /**
     * Constructor: it expects the file path, the generation rate and the parallelism degree.
     * @param file path to the input data file
     * @param gen_rate if the argument value is -1 then the spout generates tuples at
     *                 the maximum rate possible (measure the bandwidth under this assumption);
     *                 if the argument value is different from -1 then the spout generates
     *                 tuples at the rate given by this parameter (measure the latency given
     *                 this generation rate)
     * @param p_deg source parallelism degree
     */
    FileParserSpout(String file, int gen_rate, int p_deg, long _runTimeSec) {
        file_path = file;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        nt_execution = 0;       // number of generations of the dataset
        index = 0;
        epoch = 0;
        runTimeSec = _runTimeSec;
        waitForShutdown = false;
        lastTupleTs = 0;
        date = new ArrayList<>();
        time = new ArrayList<>();
        epoc = new ArrayList<>();
        devices = new ArrayList<>();
        temperature = new ArrayList<>();
        humidity = new ArrayList<>();
        light = new ArrayList<>();
        voltage = new ArrayList<>();
    }

    // open method
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;
        // set value field to be monitored
        value_field = config.getString(Conf.PARSER_VALUE_FIELD);
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
                nimbusClient.getClient().killTopology(SpikeDetectionConstants.DEFAULT_TOPO_NAME);
                waitForShutdown = true;
                return;
            }
            catch (TException e) {
                collector.reportError(e);
            }
        }
        // generate next tuple
        long timestamp = System.nanoTime();
        collector.emit(new Values(devices.get(index), data.get(index), timestamp));
        generated++;
        index++;
        lastTupleTs = timestamp;
        if (rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / rate) * 1e9);
            active_delay(delay_nsec);
        }
        // check the dataset boundaries
        if (index >= devices.size()) {
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
        outputFieldsDeclarer.declare(new Fields(Field.DEVICE_ID, Field.VALUE, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------//

    /**
     * Parse sensors' data and populate the state of the spout.
     * The parsing phase splits each line of the input dataset extracting date and time, deviceID
     * and information provided by the sensor about temperature, humidity, light and voltage.
     * 
     * Example of the result obtained by parsing one line:
     *  0 = "2004-03-31"
     *  1 = "03:38:15.757551"
     *  2 = "2"
     *  3 = "1"
     *  4 = "122.153"
     *  5 = "-3.91901"
     *  6 = "11.04"
     *  7 = "2.03397"
     */
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
                } else
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
}
