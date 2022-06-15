/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
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

package VoipStream;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineReaderSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(LineReaderSpout.class);

    private SpoutOutputCollector spoutOutputCollector;

    private String path;
    private long runTimeSec;
    private long gen_rate;
    private List<String> data;

    private boolean waitForShutdown;
    private int index;
    private int offset;
    private int stride;
    private long counter;
    private Sampler throughput;
    private long epoch, lastTupleTs;

    public LineReaderSpout(long runTimeSec, long _gen_rate, String path) {
        this.runTimeSec = runTimeSec;
        this.gen_rate = _gen_rate;
        this.path = path;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "line"));
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        // initialize
        this.spoutOutputCollector = spoutOutputCollector;
        data = new ArrayList<>();
        offset = topologyContext.getThisTaskIndex();
        stride = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        index = offset;
        counter = 0;
        throughput = new Sampler();

        // read the whole dataset
        try {
            readAll();
        } catch (IOException e) {
            spoutOutputCollector.reportError(e);
        }
    }

    @Override
    public void nextTuple() {
        // check termination
        if (waitForShutdown) {
            return;
        } else if (epoch > 0 && (System.nanoTime() - epoch) / 1e9 > runTimeSec) {
            try {
                LOG.info("Killing the topology");

                // initiate topology shutdown from inside
                NimbusClient nimbusClient = NimbusClient.getConfiguredClient(Utils.readStormConfig());
                nimbusClient.getClient().killTopology(Topology.TOPOLOGY_NAME);
                waitForShutdown = true;
                return;
            } catch (TException e) {
                spoutOutputCollector.reportError(e);
            }
        }

        if (gen_rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
            active_delay(delay_nsec);
        }

        // fetch the next item
        if (index >= data.size()) {
            index = offset;
        }
        String line = data.get(index);

        // send the tuple
        long timestamp = System.nanoTime();
        Values out = new Values(timestamp, line);
        spoutOutputCollector.emit(out);
        LOG.debug("tuple out: {}", out);
        lastTupleTs = timestamp;
        index += stride;
        counter++;

        // update the epoch the first time
        if (counter == 1) {
            epoch = System.nanoTime();
        }
    }

    @Override
    public void close() {
        double rate = counter / ((lastTupleTs - epoch) / 1e9); // per second
        throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    private void readAll() throws IOException {
        // read the whole file line by line
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                data.add(line);
            }
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
