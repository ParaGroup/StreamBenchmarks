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

package WordCount;

import Util.Log;
import Util.Sampler;
import java.io.File;
import java.util.Map;
import java.util.Random;
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
import Constants.WordCountConstants;
import Constants.WordCountConstants.*;
import Constants.WordCountConstants.Field;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.task.TopologyContext;
import com.google.common.collect.ImmutableMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

/** 
 *  @author  Alessandra Fais
 *  @version July 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  words, parsing it and generating a stream of lines toward the
 *  SplitterBolt. In alternative it generates a stream of random
 *  sentences.
 */ 
public class FileParserSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(FileParserSpout.class);
    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;

    private String file_path;
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
    private ArrayList<String> lines;    // state of the spout (contains parsed data)
    private long bytes;                 // accumulates the amount of bytes emitted

    /**
     * Constructor: it expects the source type, the file path, the generation rate and the parallelism degree.
     * @param source_type can be file or generator
     * @param file path to the input data file (null if the source type is generator)
     * @param gen_rate if the argument value is -1 then the spout generates tuples at
     *                 the maximum rate possible (measure the bandwidth under this assumption);
     *                 if the argument value is different from -1 then the spout generates
     *                 tuples at the rate given by this parameter (measure the latency given
     *                 this generation rate); this value is expressed in MB/s
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
        lines = new ArrayList<>();
        bytes = 0;                  // total number of bytes emitted
    }

    // open method
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;
        // save tuples as a state
        parseDataset();
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
                nimbusClient.getClient().killTopology(WordCountConstants.DEFAULT_TOPO_NAME);
                waitForShutdown = true;
                return;
            }
            catch (TException e) {
                collector.reportError(e);
            }
        }
        // generate next tuple
        long timestamp = System.nanoTime();
        collector.emit(new Values(lines.get(index), timestamp));
        bytes += lines.get(index).getBytes().length;
        generated++;
        index++;
        lastTupleTs = timestamp;
        if (rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / rate) * 1e9);
            active_delay(delay_nsec);
        }
        // check the dataset boundaries
        if (index >= lines.size()) {
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
        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);
        /*LOG.info("[Source] execution time: " + t_elapsed +
                 " ms, generations: " + nt_execution +
                 ", generated: " + generated + " (lines) " + (bytes / 1048576) +
                 " (MB), bandwidth: " + generated / (t_elapsed / 1000) +
                 " (lines/s) " + formatted_mbs + " (MB/s)");*/
        //throughput.add(mbs);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " lines/second, " + formatted_mbs + " MB/s");
    }

    // declareOutputFields METHOD
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.LINE, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------//

    private void parseDataset() {
        try {
            Scanner scan = new Scanner(new File(file_path));
            while (scan.hasNextLine()) {
                lines.add(scan.nextLine());
            }
            scan.close();
        } catch (FileNotFoundException | NullPointerException e) {
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
