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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import util.Configuration;
import util.Log;
import util.MetricGroup;
import util.ThroughputCounter;
import org.apache.flink.api.common.JobExecutionResult;
import java.util.concurrent.TimeUnit;

public class Topology {
    private static final Logger LOG = Log.get(Topology.class);

    public static void submit(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration) {
        try {
            // set up chaining
            boolean chaining = configuration.getTree().get("chaining").booleanValue();
            if (!chaining) {
                LOG.info("Chaining is disabled");
                streamExecutionEnvironment.disableOperatorChaining();
            }
            else {
                boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();
                if (aggressiveChaining) {
                    LOG.info("Chaining is enabled in an aggressive manner (Flink only)");
                }
                else
                {
                    LOG.info("Chaining is enabled");
                }
            }

            // run the topology
            LOG.info("Submitting topology");
            JobExecutionResult result = streamExecutionEnvironment.execute();
            LOG.info("Exiting");
            // measure throughput
            double throughput = (double) (ThroughputCounter.getValue() / result.getNetRuntime(TimeUnit.SECONDS));
            LOG.info("Measured throughput: " + throughput + " tuples/second");
            // dump the metrics
            LOG.info("Dumping metrics");
            MetricGroup.dumpAll();

        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static int getParallelismHint(Configuration configuration, String name) {
        JsonNode jsonNode = configuration.getTree().get(name);
        int parallelismHint = jsonNode == null ? 1 : jsonNode.numberValue().intValue();
        LOG.info("NODE: {} ({})", name, parallelismHint);
        return parallelismHint;
    }
}
