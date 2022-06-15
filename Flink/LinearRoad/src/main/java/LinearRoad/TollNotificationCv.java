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

package LinearRoad;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CountTuple;
import common.SegmentIdentifier;
import common.TollNotification;
import util.Log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TollNotificationCv extends RichFlatMapFunction<Tuple2<Long, CountTuple>, Tuple2<Long, TollNotification>> {
    private static final Logger LOG = Log.get(TollNotificationCv.class);

    //////////
    /**
     * Buffer for accidents.
     */
    private Set<SegmentIdentifier> currentMinuteAccidents = new HashSet<>();
    /**
     * Buffer for accidents.
     */
    private Set<SegmentIdentifier> previousMinuteAccidents = new HashSet<>();
    /**
     * Buffer for car counts.
     */
    private Map<SegmentIdentifier, Integer> currentMinuteCounts = new HashMap<>();
    /**
     * Buffer for car counts.
     */
    private Map<SegmentIdentifier, Integer> previousMinuteCounts = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<SegmentIdentifier, Integer> currentMinuteLavs = new HashMap<>();
    /**
     * Buffer for LAV values.
     */
    private Map<SegmentIdentifier, Integer> previousMinuteLavs = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;
    //////////

    @Override
    public void flatMap(Tuple2<Long, CountTuple> tuple, Collector<Tuple2<Long, TollNotification>> collector) throws Exception {
        LOG.debug("TollNotificationCv");

        long timestamp = tuple.f0;

        collector.collect(new Tuple2<>(timestamp, new TollNotification()));//as an indication.

        CountTuple inputCountTuple = tuple.f1;

        this.checkMinute(inputCountTuple.minuteNumber);

        if (inputCountTuple.isProgressTuple()) {
            return;
        }

        SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
        segmentIdentifier.xway = inputCountTuple.xway;
        segmentIdentifier.segment = inputCountTuple.segment;
        segmentIdentifier.direction = inputCountTuple.direction;
        this.currentMinuteCounts.put(segmentIdentifier, inputCountTuple.count);
    }

    private void checkMinute(short minute) {
        //due to the tuple may be send in reverse-order, it may happen that some tuples are processed too late.
//        assert (minute >= this.currentMinute);

        if (minute < this.currentMinute) {
            //restart..
            currentMinute = minute;
        }
        if (minute > this.currentMinute) {
            this.currentMinute = minute;
            this.previousMinuteAccidents = this.currentMinuteAccidents;
            this.currentMinuteAccidents = new HashSet<>();
            this.previousMinuteCounts = this.currentMinuteCounts;
            this.currentMinuteCounts = new HashMap<>();
            this.previousMinuteLavs = this.currentMinuteLavs;
            this.currentMinuteLavs = new HashMap<>();
        }
    }
}
