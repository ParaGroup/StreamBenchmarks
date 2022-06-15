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
import java.util.*;
import org.slf4j.Logger;
import Constants.YSBConstants;
import org.slf4j.LoggerFactory;
import Constants.YSBConstants.*;
import Util.config.Configuration;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

// class WinAggregateBolt
public class WinAggregateBolt extends BaseRichBolt {
    private static final Logger LOG = Log.get(WinAggregateBolt.class);
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    HashMap<String, Window> winSet;
    private long initialTime;
    private long discarded;

    // constructor
    WinAggregateBolt(int p_deg, long _initialTime) {
        par_deg = p_deg;     // bolt parallelism degree
        winSet = new HashMap<>();
        initialTime = _initialTime;
        discarded = 0;
    }

    // prepare method
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    // execute method
    @Override
    public void execute(Tuple tuple) {
        String cmp_id = tuple.getStringByField(Field.CMP_ID);
        String ad_id = tuple.getStringByField(Field.AD_ID);
        long ts = tuple.getLongByField(Field.TIMESTAMP);
        if (winSet.containsKey(cmp_id)) {
            // get the current window of key cmp_id
            Window win = winSet.get(cmp_id);
            if (ts >= win.end_ts) { // window is triggered
                collector.emit(new Values(cmp_id, win.count, ts));
                win.count = 1;
                win.start_ts = (long) ((((ts - initialTime) / 10e09) * 10e09) + initialTime);
                win.end_ts = (long) (((((ts - initialTime) / 10e09) + 1) * 10e09) + initialTime);
            }
            else if (ts >= win.start_ts) { // window is not triggered
                win.count++;
            }
            else { // tuple belongs to a previous already triggered window -> it is discarded!
                discarded++;
            }
        }
        else { // create the first window of the key cmp_id
            winSet.put(cmp_id, new Window(1, initialTime, (long) (initialTime + 10e09)));
        }
        //collector.ack(tuple);
        processed++;
        t_end = System.nanoTime();
    }

    // cleanup method
    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[WinAggregate] execution time: " + t_elapsed +
                " ms, processed: " + processed +
                ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/
        LOG.info("[WinAggregate] dropped tuples: " + discarded);
    }

    // declareOutputFields
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.CMP_ID, Field.COUNTER, Field.TIMESTAMP));
    }
}

// window class
class Window {
    public long count;
    public long start_ts;
    public long end_ts;

    public Window(long _count, long _start_ts, long _end_ts) {
        count = _count;
        start_ts = _start_ts;
        end_ts = _end_ts;
    }
}
