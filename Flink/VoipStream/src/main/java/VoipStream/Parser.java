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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

public class Parser extends RichMapFunction<
        Tuple2<Long, String>,
        Tuple5<Long, String, String, Long, CallDetailRecord>> {
    private static final Logger LOG = Log.get(Parser.class);

    @Override
    public Tuple5<Long, String, String, Long, CallDetailRecord> map(Tuple2<Long, String> tuple) {
        LOG.debug("tuple in: {}", tuple);

        // fetch values from the tuple
        long timestamp = tuple.f0;
        String line = tuple.f1;

        // parse the line
        CallDetailRecord cdr = new CallDetailRecord(line);

        Tuple5<Long, String, String, Long, CallDetailRecord> out = new Tuple5<>(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, cdr);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
