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

import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

// class SyntheticSink
public class SyntheticSink extends RichSinkFunction<Batch> {
    private int received;

    // Constructor
    public SyntheticSink() {
        received = 0;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {}

    // invoke method
    @Override
    public void invoke(Batch input, Context context) throws Exception {
        for (int i=0; i<input.getSize(); i++) {
            if (input.flags[i] == 1) {
                received++;
                // System.out.println("[SINK] Received result with id: " + input.identifiers[i] + ", v1: " + input.v1[i] + ", v2: " + input.v2[i] + ", v3: " + input.v3[i] + ", v4: " + input.v4[i]);
            }
        }
    }

    // close method
    @Override
    public void close() {
        System.out.println("[SINK] Received " + received + " results over the whole execution");
    }
}
