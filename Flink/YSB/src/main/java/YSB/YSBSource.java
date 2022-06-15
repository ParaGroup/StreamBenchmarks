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
import java.util.Random;
import org.slf4j.Logger;
import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import Util.ThroughputCounter;
import org.slf4j.LoggerFactory;
import Constants.YSBConstants.Conf;
import Constants.YSBConstants.Field;
import java.io.FileNotFoundException;
import static Constants.BaseConstants.*;
import org.apache.flink.streaming.api.watermark.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// class YSBSource
public class YSBSource extends RichParallelSourceFunction<YSB_Event> {
    private static final Logger LOG = Log.get(YSBSource.class);
    private Integer rate;
    private long generated;
    private int par_deg;
    private long runTimeSec;
    private Sampler throughput;
    private boolean isRunning;
    // YSB variables
    private List<CampaignAd> campaigns;
    private final static String CAMPAIGNS_TOPIC = "campaigns";
    private final static String EVENTS_TOPIC = "events";
    private final static String OUTPUT_TOPIC = "output";
    private final static List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    private final static List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");
    private int n_keys;
    private String uuid;
    private int adTypeLength;
    private int eventTypeLength;
    private int campaignLength;
    private int i;
    private int j;
    private int k;
    private long ts;

    // constructor
    YSBSource(int gen_rate, int p_deg, long _runTimeSec, List<CampaignAd> _campaigns, int _n_keys) {
        par_deg = p_deg;
        campaigns = _campaigns;
        rate = gen_rate;        // number of tuples per second
        generated = 0;          // total number of generated tuples
        runTimeSec = _runTimeSec;
        isRunning = true;
        n_keys = _n_keys;
        adTypeLength = 5;
        eventTypeLength = 3;
        campaignLength = campaigns.size();
        ts = System.currentTimeMillis();
        i = 0;
        j = 0;
        k = 0;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws IOException {
        throughput = new Sampler();
    }

    // run method
    @Override
    public void run(SourceContext<YSB_Event> ctx) throws Exception {
        long epoch = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - epoch < runTimeSec * 1e9) && isRunning) {
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
            String ad_id = (campaigns.get(i)).ad_id; // ad id for the current event index
            String ad_type = AD_TYPES.get(j); // current adtype for event index
            String event_type = EVENT_TYPES.get(k); // current event type for event index
            String ip = "255.255.255.255";
            long ts = System.nanoTime();
            long timestamp = System.currentTimeMillis();
            ctx.collectWithTimestamp(new YSB_Event(uuid, uuid, ad_id, ad_type, event_type, ts, ip), timestamp);
            if (generated % 1000 == 0)
                ctx.emitWatermark(new Watermark(timestamp));
            generated++;
            if (rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            // set the starting time
            if (generated == 1) {
                epoch = System.nanoTime();
            }
        }
        // terminate the generation
        isRunning = false;
        // dump metric
        double rate = generated / ((System.nanoTime() - epoch) / 1e9); // per second
        long t_elapsed = (long) ((System.nanoTime() - epoch) / 1e6);  // elapsed time in milliseconds
        /*LOG.info("[Source] execution time: " + t_elapsed +
                    " ms, generated: " + generated +
                    " , bandwidth: " + generated / (t_elapsed / 1000) + " tuples/second");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        ThroughputCounter.add(generated);
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
