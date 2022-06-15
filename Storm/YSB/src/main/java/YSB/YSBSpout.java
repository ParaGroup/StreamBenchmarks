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
import Util.Sampler;
import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import Util.MetricGroup;
import java.util.Scanner;
import java.util.ArrayList;
import Constants.YSBConstants;
import org.slf4j.LoggerFactory;
import Constants.YSBConstants.*;
import Util.config.Configuration;
import org.apache.storm.utils.Utils;
import Constants.YSBConstants.Field;
import java.io.FileNotFoundException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.task.TopologyContext;
import com.google.common.collect.ImmutableMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

// class YSBSpout
public class YSBSpout extends BaseRichSpout {
    private static final Logger LOG = Log.get(YSBSpout.class);
    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    private Integer rate;
    private int par_deg;
    private long epoch;
    private long runTimeSec;
    private boolean waitForShutdown;
    private long lastTime;
    private Sampler throughput;
    // YSB variables
    private List<CampaignAd> campaigns;
    private final static String CAMPAIGNS_TOPIC = "campaigns";
    private final static String EVENTS_TOPIC = "events";
    private final static String OUTPUT_TOPIC = "output";
    private final static List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    private final static List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");
    private String uuid;
    private int adTypeLength;
    private int eventTypeLength;
    private int campaignLength;
    private int i;
    private int j;
    private int k;
    private long ts;
    private long generated;
    private long initialTime;

    // constructor
    YSBSpout(int gen_rate, int p_deg, List<CampaignAd> _campaigns, long _runTimeSec, long _initialTime) {
        par_deg = p_deg;
        campaigns = _campaigns;
        rate = gen_rate;        // number of tuples per second
        generated = 0;          // total number of generated tuples
        epoch = 0;
        runTimeSec = _runTimeSec;
        waitForShutdown = false;
        lastTime = 0;
        initialTime = _initialTime;
    }

    // open method
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        config = Configuration.fromMap(conf);
        collector = spoutOutputCollector;
        context = topologyContext;
        generated = 0;
        uuid = UUID.randomUUID().toString(); // used as a dummy value for all events, based on ref code
        adTypeLength = AD_TYPES.size();;
        eventTypeLength = EVENT_TYPES.size();
        campaignLength = campaigns.size();
        i = 0;
        j = 0;
        k = 0;
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
                nimbusClient.getClient().killTopology(YSBConstants.DEFAULT_TOPO_NAME);
                waitForShutdown = true;
                return;
            }
            catch (TException e) {
                collector.reportError(e);
            }
        }
        i += 1;
        j += 1;
        k += 1;
        if (i >= campaignLength) {
            i = 0;
        }
        if (j >= adTypeLength) {
            j = 0;
        }
        if (k >= eventTypeLength) {
            k = 0;
        }
        ts = initialTime + (System.nanoTime() - initialTime);
        String ad_id = (campaigns.get(i)).ad_id; // ad id for the current event index
        String ad_type = AD_TYPES.get(j); // current adtype for event index
        String event_type = EVENT_TYPES.get(k); // current event type for event index
        String ip = "255.255.255.255";
        collector.emit(new Values(uuid, uuid, ad_id, ad_type, event_type, ts, ip));
        generated++;
        lastTime = System.nanoTime();
        if (rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / rate) * 1e9);
            active_delay(delay_nsec);
        }
        // set the starting time
        if (generated == 1) {
            epoch = System.nanoTime();
        }
    }

    // close method
    @Override
    public void close() {
        double rate = generated / ((lastTime - epoch) / 1e9); // per second
        long t_elapsed = (long) ((lastTime - epoch) / 1e6);  // elapsed time in milliseconds
        /*LOG.info("[Source] execution time: " + t_elapsed +
                " ms, generated: " + generated +
                ", bandwidth: " + generated / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        LOG.info("Measured throughput: " + (int) rate + " tuples/second");
    }

    // declareOutputFields method
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.UUID, Field.UUID2, Field.AD_ID, Field.AD_TYPE, Field.EVENT_TYPE, Field.TIMESTAMP, Field.IP));
    }

    //------------------------------ private methods ---------------------------//

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
