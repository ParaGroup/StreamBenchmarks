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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;
import util.ThroughputCounter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineReaderSource extends RichParallelSourceFunction<Tuple2<Long, String>> {
    private static final Logger LOG = Log.get(LineReaderSource.class);

    private String path;
    private long runTimeSec;
    private long gen_rate;
    private List<String> data;
    private long generated;
    private long counter;
    private Sampler throughput;

    public LineReaderSource(long runTimeSec, long _gen_rate, String path) {
        this.runTimeSec = runTimeSec;
        this.gen_rate = _gen_rate;
        this.path = path;
        generated = 0;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        // initialize
        data = new ArrayList<>();
        counter = 0;
        throughput = new Sampler();

        // read the whole dataset
        readAll();
    }

    @Override
    public void run(SourceContext<Tuple2<Long, String>> sourceContext) throws InterruptedException {
        long epoch = System.nanoTime();
        long timestamp = epoch;
        final int offset = getRuntimeContext().getIndexOfThisSubtask();
        final int stride = getRuntimeContext().getNumberOfParallelSubtasks();
        int index = 0;

        while (timestamp - epoch < runTimeSec * 1e9) {
            if (gen_rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
                active_delay(delay_nsec);
            }

            // fetch the next item
            if (index == 0|| index >= data.size()) {
                index = offset;
            }
            String line = data.get(index);

            // send the tuple
            timestamp = System.nanoTime();
            Tuple2<Long, String> out = new Tuple2<>(timestamp, line);
            sourceContext.collect(out);
            generated++;
            LOG.debug("tuple out: {}", out);
            index += stride;
            counter++;
        }

        // dump metric
        double rate = counter / ((timestamp - epoch) / 1e9); // per second
        throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        ThroughputCounter.add(generated);
    }

    @Override
    public void cancel() {
    }

    @Override
    public void close() {
    }

    private void readAll() throws IOException {
        // read the whole file line by line
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                data.add(line);
            }
        }
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }
}
