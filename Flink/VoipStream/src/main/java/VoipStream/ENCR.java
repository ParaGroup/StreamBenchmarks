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
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

public class ENCR extends RichFlatMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(ENCR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.ENCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ENCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ENCR_BUCKETS_PER_WORD;
        double beta = Constants.ENCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple, Collector<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        CallDetailRecord cdr = tuple.f5;

        boolean newCallee = tuple.f4;

        if (cdr.callEstablished && newCallee) {
            String caller = tuple.f1;
            filter.add(caller, 1, cdr.answerTimestamp);
            double rate = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double> out = new Tuple7<>(ScorerMap.ENCR, timestamp, caller, cdr.answerTimestamp, rate, cdr, tuple.f6);
            collector.collect(out);
            LOG.debug("tuple out: {}", out);
        }
    }
}
