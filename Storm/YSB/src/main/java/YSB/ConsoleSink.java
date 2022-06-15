/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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


package YSB;

import Util.Log;
import Util.Sampler;
import java.util.Map;
import org.slf4j.Logger;
import Util.MetricGroup;
import org.slf4j.LoggerFactory;
import Util.config.Configuration;
import Constants.YSBConstants.Field;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

// class ConsoleSink
public class ConsoleSink extends BaseRichBolt {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    private long t_start;
    private long t_end;
    private long processed;
    private long estimated_bw;
    private int par_deg;
    private int gen_rate;
    private Sampler latency;
    private final long samplingRate;

    // contructor
    ConsoleSink(int p_deg, int g_rate, long _samplingRate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
        samplingRate = _samplingRate;
        estimated_bw = 0;
    }

    // prepare method
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
        latency = new Sampler(samplingRate);
    }

    // execute method
    @Override
    public void execute(Tuple tuple) {
        String cmp_id = tuple.getStringByField(Field.CMP_ID);
        long counter = tuple.getLongByField(Field.COUNTER);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)(now - timestamp) / 1e3, now);
        //collector.ack(tuple);
        processed++;
        estimated_bw += counter * 3;
        t_end = System.nanoTime();
    }

    // cleanup method
    @Override
    public void cleanup() {
        if (processed == 0) {
            //LOG.info("[Sink] processed tuples: " + processed);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            /*LOG.info("[Sink] execution time: " + t_elapsed +
                    " ms, processed: " + processed +
                    ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                    ", estimated bandwidth: " + estimated_bw / (t_elapsed / 1000) +  // tuples per second
                    " tuples/s");*/
            MetricGroup.add("latency", latency);
        }
    }

    // declareOutputFields method
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.CMP_ID, Field.COUNTER, Field.TIMESTAMP));
    }
}
