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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;

public class DrainSink<T extends Tuple> extends RichSinkFunction<T> {
    private static final Logger LOG = Log.get(DrainSink.class);

    private final long samplingRate;

    private Sampler latency;

    public DrainSink(long samplingRate) {
        this.samplingRate = samplingRate;
    }

    @Override
    public void open(Configuration parameters) {
        latency = new Sampler(samplingRate);
    }

    @Override
    public void invoke(Tuple tuple, Context context) {
        LOG.debug("tuple in: {}", tuple);

        assert tuple.getArity() > 1;

        long timestamp = tuple.getField(0);
        long now = System.nanoTime();
        latency.add((now - timestamp) / 1e3, now); // microseconds
    }

    @Override
    public void close() {
        MetricGroup.add("latency", latency);
    }
}
