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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

public class PreRCR extends RichFlatMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(PreRCR.class);

    @Override
    public void flatMap(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple, Collector<Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;

        // fetch values from the tuple
        String callingNumber = tuple.f1;
        String calledNumber = tuple.f2;
        long answerTimestamp = tuple.f3;
        boolean newCallee = tuple.f4;
        CallDetailRecord cdr = tuple.f5;

        // emits the tuples twice, the key is calling then called number
        Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double> output = new Tuple8<>(null, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, tuple.f6);

        output.f0 = callingNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);


        output.f0 = calledNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);
    }
}
