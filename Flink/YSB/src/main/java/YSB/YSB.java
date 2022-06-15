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
import Util.MetricGroup;
import org.slf4j.Logger;
import java.io.IOException;
import Constants.YSBConstants;
import Util.ThroughputCounter;
import org.apache.flink.util.*;
import org.slf4j.LoggerFactory;
import Constants.BaseConstants;
import Constants.YSBConstants.Conf;
import java.util.concurrent.TimeUnit;
import Constants.YSBConstants.Component;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// class YSB
public class YSB {
    private static final Logger LOG = Log.get(YSB.class);

    // main
    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals(BaseConstants.HELP)) {
            String alert = "Parameters: --rate <value> --sampling <value> --parallelism <nSource nFilter nJoiner nAggregate nSink> [--chaining]\n";
            LOG.error(alert);
        }
        else {
            // load configuration
            ParameterTool params;
            Configuration conf;
            try {
                params = ParameterTool.fromPropertiesFile(YSB.class.getResourceAsStream(YSBConstants.DEFAULT_PROPERTIES));
                conf = params.getConfiguration();
                LOG.debug("Loaded configuration file");
            }
            catch (IOException e) {
                LOG.error("Unable to load configuration file", e);
                throw new RuntimeException("Unable to load configuration file", e);
            }
            // parse command line arguments
            boolean isCorrect = true;
            int gen_rate = -1;
            int sampling = 1;
            int source_par_deg = 1;
            int filter_par_deg = 1;
            int joiner_par_deg = 1;
            int agg_par_deg = 1;
            int sink_par_deg = 1;
            boolean isChaining = false;
            if (args.length == 10 || args.length == 11) {
                if (!args[0].equals("--rate")) {
                    isCorrect = false;
                }
                else {
                    try {
                        gen_rate = Integer.parseInt(args[1]);
                    }
                    catch (NumberFormatException e) {
                        isCorrect = false;
                    }
                }
                if (!args[2].equals("--sampling"))
                    isCorrect = false;
                else {
                    try {
                        sampling = Integer.parseInt(args[3]);
                    }
                    catch (NumberFormatException e) {
                        isCorrect = false;
                    }
                }
                if (!args[4].equals("--parallelism"))
                    isCorrect = false;
                else {
                    try {
                        source_par_deg = Integer.parseInt(args[5]);
                        filter_par_deg = Integer.parseInt(args[6]);
                        joiner_par_deg = Integer.parseInt(args[7]);
                        agg_par_deg = Integer.parseInt(args[8]);
                        sink_par_deg = Integer.parseInt(args[9]);
                    }
                    catch (NumberFormatException e) {
                        isCorrect = false;
                    }
                }
                if (args.length == 11) {
                    if (!args[10].equals("--chaining")) {
                        isCorrect = false;
                    }
                    else {
                        isChaining = true;
                    }
                }
            }
            else {
                LOG.error("Error in parsing the input arguments");
                System.exit(1);
            }
            if (!isCorrect) {
               LOG.error("Error in parsing the input arguments");
               System.exit(1);
            }
            String topology_name = YSBConstants.DEFAULT_TOPO_NAME;
            long runTimeSec = conf.getInteger(Conf.RUNTIME, 0);
            // initialize the HashMap of campaigns
            int numCampaigns = conf.getInteger(Conf.NUM_KEYS, 0);
            List<CampaignAd> campaignAdSeq = generateCampaignMapping(numCampaigns);
            HashMap<String, String> campaignLookup = new HashMap<String, String>();
            for (int i=0; i<campaignAdSeq.size(); i++) {
                campaignLookup.put((campaignAdSeq.get(i)).ad_id, (campaignAdSeq.get(i)).campaign_id);
            }

            // create the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // flush as soon as possible in throttled mode (minimize the latency)
            if (gen_rate != 0) {
                env.setBufferTimeout(0);
            }

            // tag for late tuples
            final OutputTag<Joined_Event> lateOutputTag = new OutputTag<Joined_Event>("late-data"){};

            // create the topology
            DataStream<YSB_Event> event_stream = env.addSource(new YSBSource(gen_rate, source_par_deg, runTimeSec, campaignAdSeq, numCampaigns)).setParallelism(source_par_deg);

            DataStream<YSB_Event> filtered_stream = event_stream.filter(new Filter(filter_par_deg)).setParallelism(filter_par_deg);

            DataStream<Joined_Event> joined_stream = filtered_stream.flatMap(new Joiner(campaignLookup, joiner_par_deg)).setParallelism(joiner_par_deg);

            SingleOutputStreamOperator<Aggregate_Event> agg_stream = joined_stream.keyBy("cmp_id").window(TumblingEventTimeWindows.of(Time.seconds(10))).sideOutputLateData(lateOutputTag).fold(new Aggregate_Event(), new FoldFunction<Joined_Event, Aggregate_Event> () {
                public Aggregate_Event fold(Aggregate_Event agg, Joined_Event event) {
                    agg.cmp_id = event.cmp_id;
                    agg.count++;
                    agg.timestamp = event.timestamp;
                    return agg;
                }
            }).setParallelism(agg_par_deg);

            DataStream<Joined_Event> lateStream = agg_stream.getSideOutput(lateOutputTag);

            lateStream.addSink(new DroppedSink()).setParallelism(1);

            agg_stream.addSink(new ConsoleSink(sink_par_deg, gen_rate, sampling)).setParallelism(sink_par_deg);

            // print app info
            LOG.info("Executing YSB with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * filter: " + filter_par_deg + "\n" +
                     "  * joiner: " + joiner_par_deg + "\n" +
                     "  * winAggregate: " + agg_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> filter -> joiner -> winAggregate -> sink");

            // run the topology
            try {
                // configure the environment
                if (!isChaining) {
                    env.disableOperatorChaining();
                    LOG.info("Chaining is disabled");
                }
                else {
                    LOG.info("Chaining is enabled");
                }
                // run the topology
                LOG.info("Submitting topology");
                JobExecutionResult result = env.execute();
                LOG.info("Exiting");
                // measure throughput
                double throughput = (double) (ThroughputCounter.getValue() / result.getNetRuntime(TimeUnit.SECONDS));
                LOG.info("Measured throughput: " + (int) throughput + " tuples/second");
                // dump the metrics
                LOG.info("Dumping metrics");
                MetricGroup.dumpAll();
            }
            catch (Exception e) {
                LOG.error(e.toString());
            }
        }
    }

    // generate in-memory ad_id to campaign_id map
    private static List<CampaignAd> generateCampaignMapping(int numCampaigns) {
        CampaignAd[] campaignArray = new CampaignAd[numCampaigns*10];
        for (int i=0; i<numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            for (int j=0; j<10; j++) {
                campaignArray[(10*i)+j] = new CampaignAd(UUID.randomUUID().toString(), campaign);
            }
        }
        return Arrays.asList(campaignArray);
    }
}
