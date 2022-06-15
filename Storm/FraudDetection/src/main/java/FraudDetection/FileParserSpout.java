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

package FraudDetection;

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
import Constants.FraudDetectionConstants;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import Constants.FraudDetectionConstants.*;
import org.apache.storm.task.TopologyContext;
import Constants.FraudDetectionConstants.Field;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

/** 
 *  @author Alessandra Fais
 *  @version June 2019
 *  
 *  The spout is in charge of reading the input data file, parsing it
 *  and generating the stream of records toward the FraudPredictorBolt.
 *  
 *  Format of the input file:
 *  <entityID, transactionID, transactionType>
 */ 
public class FileParserSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(FileParserSpout.class);
    private SpoutOutputCollector spoutOutputCollector;
    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    private String file_path;
    private String split_regex;
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
    // state of the spout (contains parsed data)
    private ArrayList<String> entities;
    private ArrayList<String> records;

    /**
     * Constructor: it expects the file path and the split expression needed
     * to parse the file (it depends on the format of the input data).
     * @param file path to the input data file
     * @param split split expression
     * @param gen_rate if the argument value is -1 then the spout generates tuples at
     *                 the maximum rate possible (measure the bandwidth under this assumption);
     *                 if the argument value is different from -1 then the spout generates
     *                 tuples at the rate given by this parameter (measure the latency given
     *                 this generation rate)
     * @param p_deg source parallelism degree
     */
    FileParserSpout(String file, String split, int gen_rate, int p_deg, long _runTimeSec) {
        file_path = file;
        split_regex = split;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        nt_execution = 0;       // number of generations of the dataset
        index = 0;
        epoch = 0;
        runTimeSec = _runTimeSec;
        waitForShutdown = false;
        lastTupleTs = 0;
        entities = new ArrayList<>();
        records = new ArrayList<>();
    }

    // open method
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector _spoutOutputCollector) {
        spoutOutputCollector = _spoutOutputCollector;
        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;
        throughput = new Sampler();
        // save tuples as a state
        parseDataset();
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
                nimbusClient.getClient().killTopology(FraudDetectionConstants.DEFAULT_TOPO_NAME);
                waitForShutdown = true;
                return;
            }
            catch (TException e) {
                spoutOutputCollector.reportError(e);
            }
        }
        // generate next tuple
        long timestamp = System.nanoTime();
        collector.emit(new Values(entities.get(index), records.get(index), timestamp));
        generated++;
        index++;
        lastTupleTs = timestamp;
        if (rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / rate) * 1e9);
            active_delay(delay_nsec);
        }
        // check the dataset boundaries
        if (index >= entities.size()) {
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
                 ", bandwidth: " + rate +  // tuples per second
                 " tuples/s");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    // declareOutputFields method
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.ENTITY_ID, Field.RECORD_DATA, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------//

    /**
     * Parse credit cards' transactions data and populate the state of the spout.
     * The parsing phase splits each line of the input dataset in 2 parts:
     * - first string identifies the customer (entityID)
     * - second string contains the transactionID and the transaction type
     */
    private void parseDataset() {
        try {
            Scanner scan = new Scanner(new File(file_path));
            while (scan.hasNextLine()) {
                String[] line = scan.nextLine().split(split_regex, 2);
                entities.add(line[0]);
                records.add(line[1]);
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
