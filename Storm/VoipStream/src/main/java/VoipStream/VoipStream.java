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

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import util.Configuration;
import org.slf4j.Logger;
import util.Log;
import java.io.IOException;

public class VoipStream {
    private static final Logger LOG = Log.get(VoipStream.class);

    // Main
    public static void main(String[] args) throws IOException {
        Configuration configuration = Configuration.fromArgs(args);
        String variant = configuration.getTree().get("variant").textValue();

        // run the variant
        switch (variant) {
            case "default":
                runDefaultVariant(configuration);
                break;
            case "reordering":
                runReorderingVariant(configuration);
                break;
            case "forwarding":
                runForwardingVariant(configuration);
                break;
            default:
                System.err.println("Unknown variant");
                System.exit(1);
        }
    }

    static void runDefaultVariant(Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();

        // print app info
        LOG.info("Executing VoipStream (default) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);
        topologyBuilder.setSpout("source", new LineReaderSpout(runTime, gen_rate, datasetPath));

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("dispatcher");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("ecr", new ECR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("encr", new ENCR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL())
                .fieldsGrouping("ecr", new Fields("calling_number"))
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR())
                .fieldsGrouping("rcr", new Fields("calling_number"))
                .fieldsGrouping("ecr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink(samplingRate))
                .shuffleGrouping("score");

        // start!
        Topology.submit(topologyBuilder, configuration);
    }

    static void runReorderingVariant(Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();

        // print app info
        LOG.info("Executing VoipStream (reordering) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);
        topologyBuilder.setSpout("source", new LineReaderSpout(runTime, gen_rate, datasetPath));

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));
        topologyBuilder.setBolt("ecr", new ECR_Reordering())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("ecr");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("encr", new ENCR_Reordering())
                .fieldsGrouping("ecr", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL())
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("ecr");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR_Reordering())
                .fieldsGrouping("rcr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink(samplingRate))
                .shuffleGrouping("score");

        // start!
        Topology.submit(topologyBuilder, configuration);
    }

    static void runForwardingVariant(Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();

        // print app info
        LOG.info("Executing VoipStream (forwarding) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);
        topologyBuilder.setSpout("source", new LineReaderSpout(runTime, gen_rate, datasetPath));

        topologyBuilder.setBolt("parser", new Parser())
                .shuffleGrouping("source");
        topologyBuilder.setBolt("dispatcher", new Dispatcher())
                .fieldsGrouping("parser", new Fields("calling_number", "called_number"));

        topologyBuilder.setBolt("ct24", new CT24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("global_acd", new GlobalACD(), 1) // XXX fixed parallelism degree
                .globalGrouping("dispatcher");
        LOG.info("BOLT: {} ({})", "global_acd", 1);
        topologyBuilder.setBolt("ecr24", new ECR24())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("acd", new ACD())
                .fieldsGrouping("ct24", new Fields("calling_number"))
                .allGrouping("global_acd")
                .fieldsGrouping("ecr24", new Fields("calling_number"));

        topologyBuilder.setBolt("ecr", new ECR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("encr", new ENCR())
                .fieldsGrouping("dispatcher", new Fields("calling_number"));
        topologyBuilder.setBolt("url", new URL_Forwarding())
                .fieldsGrouping("fofir", new Fields("calling_number"))
                .fieldsGrouping("encr", new Fields("calling_number"));

        topologyBuilder.setBolt("pre_rcr", new PreRCR())
                .shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("rcr", new RCR())
                .fieldsGrouping("pre_rcr", new Fields("key"));
        topologyBuilder.setBolt("fofir", new FoFiR_Forwarding())
                .fieldsGrouping("rcr", new Fields("calling_number"))
                .fieldsGrouping("ecr", new Fields("calling_number"));

        topologyBuilder.setBolt("score", new Score())
                .fieldsGrouping("acd", new Fields("calling_number"))
                .fieldsGrouping("url", new Fields("calling_number"));

        topologyBuilder.setBolt("sink", new DrainSink(samplingRate))
                .shuffleGrouping("score");

        // start!
        Topology.submit(topologyBuilder, configuration);
    }
}
