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
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

// class Joiner
public class Joiner extends RichFlatMapFunction<YSB_Event, Joined_Event> {
    private static final Logger LOG = Log.get(Joiner.class);
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    HashMap<String, String> campaignLookup;

    // constructor
    Joiner(HashMap<String, String> _campaignLookup, int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
        campaignLookup = _campaignLookup;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
    }

    // flatmap method
    @Override
    public void flatMap(YSB_Event input, Collector<Joined_Event> output) {
        String ad_id = input.ad_id;
        long ts = input.timestamp;
        String campaign_id = campaignLookup.get(ad_id);
        if (campaign_id != null) {
            output.collect(new Joined_Event(campaign_id, ad_id, ts));
        }
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[Joiner] execution time: " + t_elapsed +
                            " ms, processed: " + processed +
                            ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");*/
    }
}
