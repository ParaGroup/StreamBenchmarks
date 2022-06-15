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

package SyntheticGPUApp;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// class SyntheticGPUAppJob
public class SyntheticGPUAppJob {
    public static void main(String[] args) throws Exception {
        int parallelism = 1;
        int batch_size = 1;
        int app_runtime_sec = 60;
        if (args.length != 3) {
            System.out.println("Usage: <parallelism> <batch_size> <app_runtime_sec>");
            System.exit(1);
        }
        else {
            parallelism = Integer.parseInt(args[0]);
            if (parallelism < 1) {
                System.out.println("Error: parallelism parameter cannot be lower than 1!");
                System.exit(1);
            }
            batch_size = Integer.parseInt(args[1]);
            if (batch_size < 1) {
                System.out.println("Error: batch_size parameter cannot be lower than 1!");
                System.exit(1);
            }
            app_runtime_sec = Integer.parseInt(args[2]);
            if (batch_size < 1) {
                System.out.println("Error: app_runtime_sec parameter cannot be lower than 1!");
                System.exit(1);
            }
        }
        // create the streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the Source operator
        DataStream<Batch> tuples = env.addSource(new SyntheticSource(batch_size, app_runtime_sec)).setParallelism(parallelism);

        // create the Filter operator
        DataStream<Batch> filteredTuples = tuples.flatMap(new SyntheticFilter(batch_size)).setParallelism(parallelism);

        // create the Map operator
        // DataStream<Batch> computedTuples = filteredTuples.map(new SyntheticMap()).setParallelism(parallelism);

        // create the Sink operator
        filteredTuples.addSink(new SyntheticSink()).setParallelism(parallelism);

        // run the topology
        System.out.println("Submitting topology with parallelism: " + parallelism + ", batch_size: " + batch_size + " and runtime_length: " + app_runtime_sec);
        env.execute();
    }
}
