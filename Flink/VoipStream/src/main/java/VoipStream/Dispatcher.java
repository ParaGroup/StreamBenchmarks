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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.BloomFilter;
import common.CallDetailRecord;
import common.Constants;
import util.Log;

public class Dispatcher extends RichMapFunction<
        Tuple5<Long, String, String, Long, CallDetailRecord>,
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(Dispatcher.class);

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private double cycleThreshold;

    @Override
    public void open(Configuration parameters) {

        int approxInsertSize = Constants.VAR_DETECT_APROX_SIZE;
        double falsePostiveRate = Constants.VAR_DETECT_ERROR_RATE;
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        cycleThreshold = detector.size() / Math.sqrt(2);
    }

    @Override
    public Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> map(Tuple5<Long, String, String, Long, CallDetailRecord> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;

        // fetch values from the tuple
        CallDetailRecord cdr = tuple.f4;
        String key = String.format("%s:%s", cdr.callingNumber, cdr.calledNumber);
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> out = new Tuple7<>(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, newCallee, cdr, -1.0);
        LOG.debug("tuple out: {}", out);
        return out;
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}
