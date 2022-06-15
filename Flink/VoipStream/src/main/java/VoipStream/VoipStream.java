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

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import common.CallDetailRecord;
import util.Configuration;
import java.io.IOException;
import org.slf4j.Logger;
import util.Log;

public class VoipStream {
    private static final Logger LOG = Log.get(VoipStream.class);

    // Main
    public static void main(String[] args) throws IOException {
        Configuration configuration = Configuration.fromArgs(args);
        String variant = configuration.getTree().get("variant").textValue();

        // initialize the environment
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // flush as soon as possible in throttled mode (minimize the latency)
        if (configuration.getTree().get("gen_rate").numberValue().longValue() != 0) {
            streamExecutionEnvironment.setBufferTimeout(0);
        }

        // run the variant
        switch (variant) {
            case "default":
                runDefaultVariant(streamExecutionEnvironment, configuration);
                break;
            case "reordering":
                runReorderingVariant(streamExecutionEnvironment, configuration);
                break;
            case "forwarding":
                runForwardingVariant(streamExecutionEnvironment, configuration);
                break;
            default:
                System.err.println("Unknown variant");
                System.exit(1);
        }
    }

    private static void runDefaultVariant(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();
        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        // print app info
        LOG.info("Executing VoipStream (default) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        SingleOutputStreamOperator<Tuple2<Long, String>> source = streamExecutionEnvironment
                .addSource(new LineReaderSource(runTime, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"));

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>> dispatcher = source
                .map(new Parser()).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> rcr = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> ecr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> fofir = rcr.union(ecr)
                .flatMap(new FoFiR()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> encr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> url = ecr.union(encr)
                .flatMap(new URL()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple5<Long, String, Long, Double, CallDetailRecord>> score = acd.union(fofir, url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(samplingRate)).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration);
    }

    private static void runReorderingVariant(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();
        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        // print app info
        LOG.info("Executing VoipStream (reordering) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        SingleOutputStreamOperator<Tuple2<Long, String>> source = streamExecutionEnvironment
                .addSource(new LineReaderSource(runTime, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"));

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>> dispatcher = source
                .map(new Parser()).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"))
                .keyBy(1) // calling number
                .map(new ECR_reordering()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"));

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> fofir = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2) // calling number
                .flatMap(new FoFiR_reordering()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> url = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR_reordering()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2) // calling number
                .flatMap(new URL()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple5<Long, String, Long, Double, CallDetailRecord>> score = acd.union(fofir, url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(samplingRate)).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration);
    }

    private static void runForwardingVariant(StreamExecutionEnvironment streamExecutionEnvironment, Configuration configuration) {
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();
        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        // print app info
        LOG.info("Executing VoipStream (forwarding) with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 15 operators");

        // prepare the topology
        SingleOutputStreamOperator<Tuple2<Long, String>> source = streamExecutionEnvironment
                .addSource(new LineReaderSource(runTime, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"));

        if (!aggressiveChaining) {
            source.startNewChain();
        }

        SingleOutputStreamOperator<Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>> dispatcher = source
                .map(new Parser()).name("parser")
                .setParallelism(Topology.getParallelismHint(configuration, "parser"))
                .keyBy(1, 2) // both call numbers
                .map(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ct24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new CT24()).name("ct24")
                .setParallelism(Topology.getParallelismHint(configuration, "ct24"))
                .keyBy(2); // calling number

        DataStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> globalAcd = dispatcher
                .keyBy(1) // calling number
                .map(new GlobalACD()).name("global_acd")
                .setParallelism(1) // XXX fixed parallelism degree
                .broadcast();
        LOG.info("NODE: {} ({})", "global_acd", 1);

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> ecr24 = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR24()).name("ecr24")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr24"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> acd = ct24.union(globalAcd, ecr24)
                .flatMap(new ACD()).name("acd")
                .setParallelism(Topology.getParallelismHint(configuration, "acd"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> preRcr = dispatcher
                .flatMap(new PreRCR()).name("pre_rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "pre_rcr"));

        if (!aggressiveChaining) {
            preRcr.disableChaining();
        }

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> rcr = preRcr
                .keyBy(0) // key field
                .flatMap(new RCR()).name("rcr")
                .setParallelism(Topology.getParallelismHint(configuration, "rcr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> ecr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ECR()).name("ecr")
                .setParallelism(Topology.getParallelismHint(configuration, "ecr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> fofir = rcr.union(ecr)
                .flatMap(new FoFiR_forwarding()).name("fofir")
                .setParallelism(Topology.getParallelismHint(configuration, "fofir"))
                .keyBy(2); // calling number

        KeyedStream<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>, Tuple> encr = dispatcher
                .keyBy(1) // calling number
                .flatMap(new ENCR()).name("encr")
                .setParallelism(Topology.getParallelismHint(configuration, "encr"))
                .keyBy(2); // calling number

        KeyedStream<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>, Tuple> url = fofir.union(encr)
                .flatMap(new URL_forwarding()).name("url")
                .setParallelism(Topology.getParallelismHint(configuration, "url"))
                .keyBy(2); // calling number

        SingleOutputStreamOperator<Tuple5<Long, String, Long, Double, CallDetailRecord>> score = acd.union(url)
                .map(new Score()).name("score")
                .setParallelism(Topology.getParallelismHint(configuration, "score"));

        score
                .addSink(new DrainSink<>(samplingRate)).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration);
    }
}
