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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import util.Log;

import java.util.Map;

public class ACD extends RichFlatMapFunction<
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>,
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> {
    private static final Logger LOG = Log.get(ACD.class);

    private double avg;
    private ScorerMap scorerMap;

    @Override
    public void open(Configuration parameters) {

        scorerMap = new ScorerMap(new int[]{ScorerMap.CT24, ScorerMap.ECR24});
    }

    @Override
    public void flatMap(Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> tuple, Collector<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> collector) throws Exception {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        int source = tuple.f0;

        if (source == ScorerMap.GlobalACD) {
            avg = tuple.f4;
        } else {
            CallDetailRecord cdr = tuple.f5;
            String number = tuple.f2;
            long answerTimestamp = tuple.f3;
            double rate = tuple.f4;

            String key = String.format("%s:%d", number, answerTimestamp);

            Map<String, ScorerMap.Entry> map = scorerMap.getMap();
            if (map.containsKey(key)) {
                ScorerMap.Entry entry = map.get(key);
                entry.set(source, rate);

                if (entry.isFull()) {
                    // calculate the score for the ratio
                    double ratio = (entry.get(ScorerMap.CT24) / entry.get(ScorerMap.ECR24)) / avg;
                    double score = ScorerMap.score(Constants.ACD_THRESHOLD_MIN, Constants.ACD_THRESHOLD_MAX, ratio);

                    Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> out = new Tuple6<>(ScorerMap.ACD, timestamp, number, answerTimestamp, score, cdr);
                    collector.collect(out);
                    LOG.debug("tuple out: {}", out);

                    map.remove(key);
                }
            } else {
                ScorerMap.Entry entry = scorerMap.newEntry();
                entry.set(source, rate);
                map.put(key, entry);
            }
        }
    }
}
