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
import java.io.IOException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// class SyntheticSource
public class SyntheticSource extends RichParallelSourceFunction<Batch> {
    private int batch_size;
    private int app_runtime_sec;
    private boolean isRunning;
    private Random rd;

    // Constructor
    SyntheticSource(int _batch_size, int _app_runtime_sec) {
        batch_size = _batch_size;
        app_runtime_sec = _app_runtime_sec;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws IOException {
        isRunning = true;
        rd = new Random();
    }

    // run method
    @Override
    public void run(SourceContext<Batch> ctx) throws Exception {
        long epoch = System.nanoTime();
        int counter = 0;
        Batch batch = null;
        // generation loop
        while ((System.nanoTime() - epoch < app_runtime_sec * 1e9) && isRunning) {
            if (batch == null) {
                batch = new Batch(batch_size); // create the new batch
            }

            // create a new tuple
            batch.append(counter, rd.nextFloat(), rd.nextFloat(), rd.nextFloat(), rd.nextFloat());
            // batch.append(counter, 0.89f, 0.89f, 0.89f, 0.89f); // on Jetson Nano C++ Random is extremely slow!

            counter++;
            if (batch.getSize() == batch_size) {
                ctx.collect(batch);
                batch = null;
            }
            // active_delay(100000000);
        }
        System.out.println("[SOURCE] Transmitted " + counter + " tuples");
        // terminate the generation
        isRunning = false;
    }

    // active_delay method
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
        isRunning = false;
    }

    @Override
    public void close() {}
}
