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

package TrafficMonitoring;

import Util.Log;
import Util.MetricGroup;
import org.slf4j.Logger;
import java.io.IOException;
import Util.ThroughputCounter;
import org.slf4j.LoggerFactory;
import Constants.BaseConstants;
import java.util.concurrent.TimeUnit;
import Constants.TrafficMonitoringConstants;
import Constants.TrafficMonitoringConstants.Conf;
import Constants.TrafficMonitoringConstants.City;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import Constants.TrafficMonitoringConstants.Component;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The topology entry class. The Storm compatible API is used in order to submit
 *  a Storm topology to Flink. The used Storm classes are replaced with their
 *  Flink counterparts in the Storm client code that assembles the topology.
 *  
 *  See https://ci.apache.org/projects/flink/flink-docs-stable/dev/libs/storm_compatibility.html
 */ 
public class TrafficMonitoring {
    private static final Logger LOG = Log.get(TrafficMonitoring.class);

    // main method
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals(BaseConstants.HELP)) {
           String alert = "Parameters: --rate <value> --sampling <value> --parallelism <nSource nMap-Matcher nSpeed-Calculator nSink> [--chaining]\n";
            LOG.error(alert);
        }
        else {
            // load configuration
            ParameterTool params;
            Configuration conf;
            try {
                params = ParameterTool.fromPropertiesFile(TrafficMonitoring.class.getResourceAsStream(TrafficMonitoringConstants.DEFAULT_PROPERTIES));
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
            int mm_par_deg = 1;
            int sc_par_deg = 1;
            int sink_par_deg = 1;
            boolean isChaining = false;
            if (args.length == 9 || args.length == 10) {
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
                        mm_par_deg = Integer.parseInt(args[6]);
                        sc_par_deg = Integer.parseInt(args[7]);
                        sink_par_deg = Integer.parseInt(args[8]);
                    }
                    catch (NumberFormatException e) {
                        isCorrect = false;
                    }
                }
                if (args.length == 10) {
                    if (!args[9].equals("--chaining")) {
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
            String file_path = conf.getString(Conf.SPOUT_BEIJING, "undefined");
            String topology_name = TrafficMonitoringConstants.DEFAULT_TOPO_NAME;
            long runTimeSec = conf.getInteger(Conf.RUNTIME, 0);

            // create the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // flush as soon as possible in throttled mode (minimize the latency)
            if (gen_rate != 0) {
                env.setBufferTimeout(0);
            }

            // create the topology
            DataStream<Source_Event> source_stream = env.addSource(new FileParserSource(City.BEIJING, gen_rate, source_par_deg, conf, runTimeSec)).setParallelism(source_par_deg);

            // SingleOutputStreamOperator<Source_Event> source_stream = env.addSource(new FileParserSource(City.BEIJING, gen_rate, source_par_deg, conf, runTimeSec)).setParallelism(source_par_deg);
            // source_stream.startNewChain();

            DataStream<Output_Event> road_stream = source_stream.flatMap(new MapMatchingCalculator(City.BEIJING, mm_par_deg, conf)).setParallelism(mm_par_deg);

            DataStream<Speed_Event> speed_stream = road_stream.keyBy("roadID").flatMap(new SpeedCalculator(sc_par_deg)).setParallelism(sc_par_deg);

            speed_stream.addSink(new ConsoleSink(sink_par_deg, gen_rate, sampling)).setParallelism(sink_par_deg);

            // print app info
            LOG.info("Executing TrafficMonitoring with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * map-matcher: " + mm_par_deg + "\n" +
                     "  * speed-calculator: " + sc_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> map-matcher -> speed-calculator -> sink");

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
                LOG.info("Measured throughput: " + throughput + " tuples/second");
                // dump the metrics
                LOG.info("Dumping metrics");
                MetricGroup.dumpAll();
            }
            catch (Exception e) {
                LOG.error(e.toString());
            }
        }
    }
}
