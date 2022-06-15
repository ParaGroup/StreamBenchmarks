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

package LinearRoad;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import common.AvgVehicleSpeedTuple;
import common.CountTuple;
import common.PositionReport;
import common.TollNotification;
import util.Configuration;
import org.slf4j.Logger;
import util.Log;

import java.io.IOException;

public class LinearRoad {
    private static final Logger LOG = Log.get(LinearRoad.class);

    // Main
    public static void main(String[] args) throws IOException {
        Configuration configuration = Configuration.fromArgs(args);
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();
        boolean aggressiveChaining = configuration.getTree().get("aggressive_chaining").booleanValue();

        // print app info
        LOG.info("Executing LinearRoad with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 9 operators");

        // initialize the environment
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // flush as soon as possible in throttled mode (minimize the latency)
        if (configuration.getTree().get("gen_rate").numberValue().longValue() != 0) {
            streamExecutionEnvironment.setBufferTimeout(0);
        }

        // prepare the topology
        SingleOutputStreamOperator<Tuple2<Long, PositionReport>> dispatcher = streamExecutionEnvironment
                .addSource(new LineReaderSource(runTime, gen_rate, datasetPath)).name("source")
                .setParallelism(Topology.getParallelismHint(configuration, "source"))
                .flatMap(new Dispatcher()).name("dispatcher")
                .setParallelism(Topology.getParallelismHint(configuration, "dispatcher"));

        SingleOutputStreamOperator<Tuple2<Long, AvgVehicleSpeedTuple>> averageSpeed = dispatcher
                .flatMap(new AverageSpeed()).name("average_speed")
                .setParallelism(Topology.getParallelismHint(configuration, "average_speed"));

        if (!aggressiveChaining) {
            averageSpeed.startNewChain();
        }

        SingleOutputStreamOperator<Tuple2<Long, TollNotification>> speedBranch = averageSpeed
                .flatMap(new LastAverageSpeed()).name("last_average_speed")
                .setParallelism(Topology.getParallelismHint(configuration, "last_average_speed"))
                .flatMap(new TollNotificationLas()).name("toll_notification_las")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_las"));

        ///

        SingleOutputStreamOperator<Tuple2<Long, CountTuple>> countVehicles = dispatcher
                .flatMap(new CountVehicles()).name("count_vehicles")
                .setParallelism(Topology.getParallelismHint(configuration, "count_vehicles"));

        if (!aggressiveChaining) {
            countVehicles.startNewChain();
        }

        SingleOutputStreamOperator<Tuple2<Long, TollNotification>> countBranch = countVehicles
                .flatMap(new TollNotificationCv()).name("toll_notification_cv")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_cv"));

        ///

        SingleOutputStreamOperator<Tuple2<Long, TollNotification>> positionBranch = dispatcher
                .flatMap(new TollNotificationPos()).name("toll_notification_pos")
                .setParallelism(Topology.getParallelismHint(configuration, "toll_notification_pos"));

        if (!aggressiveChaining) {
            positionBranch.startNewChain();
        }

        ///

        positionBranch.union(speedBranch, countBranch)
                .addSink(new DrainSink<>(samplingRate)).name("sink")
                .setParallelism(Topology.getParallelismHint(configuration, "sink"));

        // start!
        Topology.submit(streamExecutionEnvironment, configuration);
    }
}
