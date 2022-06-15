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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import common.VariableEWMA;
import util.Log;

// XXX include some common fields to CT24 and ECR24 since Flink tuples must be strongly typed
public class GlobalACD extends RichMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> {
    private static final Logger LOG = Log.get(GlobalACD.class);

    private VariableEWMA avgCallDuration;

    @Override
    public void open(Configuration parameters) {

        double decayFactor = Constants.ACD_DECAY_FACTOR;
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> map(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        CallDetailRecord cdr = tuple.f5;
        avgCallDuration.add(cdr.callDuration);

        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> out = new Tuple6<>(ScorerMap.GlobalACD, timestamp, cdr.callingNumber, cdr.answerTimestamp, avgCallDuration.getAverage(), cdr);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
