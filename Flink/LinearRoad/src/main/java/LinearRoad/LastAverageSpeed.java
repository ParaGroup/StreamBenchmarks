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
import common.AvgVehicleSpeedTuple;
import common.LavTuple;
import common.SegmentIdentifier;
import util.Log;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class LastAverageSpeed extends RichFlatMapFunction<Tuple2<Long, AvgVehicleSpeedTuple>, Tuple2<Long, LavTuple>> {
    private static final Logger LOG = Log.get(LastAverageSpeed.class);

    //////////
    /**
     * Holds the (at max) last five average speed value for each segment.
     */
    private final Map<SegmentIdentifier, List<Integer>> averageSpeedsPerSegment = new HashMap<>();

    /**
     * Holds the (at max) last five minute numbers for each segment.
     */
    private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<>();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
    //////////

    @Override
    public void flatMap(Tuple2<Long, AvgVehicleSpeedTuple> tuple, Collector<Tuple2<Long, LavTuple>> collector) throws Exception {
        LOG.debug("LastAverageSpeed");

        long timestamp = tuple.f0;

        // TODO apparently not used downstream...
//        Long msgId;
//        Long SYSStamp;
//        msgId = input.getLongByField(MSG_ID);
//        SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

        AvgVehicleSpeedTuple inputTuple = tuple.f1;

        Short minuteNumber = inputTuple.minute;
        short m = minuteNumber;

        this.segmentIdentifier.xway = inputTuple.xway;
        this.segmentIdentifier.segment = inputTuple.segment;
        this.segmentIdentifier.direction = inputTuple.direction;

        List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
        List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);

        if (latestAvgSpeeds == null) {
            latestAvgSpeeds = new LinkedList<>();
            this.averageSpeedsPerSegment.put((SegmentIdentifier) this.segmentIdentifier.clone(), latestAvgSpeeds);
            latestMinuteNumber = new LinkedList<>();
            this.minuteNumbersPerSegment.put((SegmentIdentifier) this.segmentIdentifier.clone(), latestMinuteNumber);
        }
        latestAvgSpeeds.add(inputTuple.avgSpeed);
        latestMinuteNumber.add(minuteNumber);

        // discard all values that are more than 5 minutes older than current minute
        while (latestAvgSpeeds.size() > 1) {
            if (latestMinuteNumber.get(0) < m - 4) {
                latestAvgSpeeds.remove(0);
                latestMinuteNumber.remove(0);
            } else {
                break;
            }
        }

        Integer lav = this.computeLavValue(latestAvgSpeeds);

        LavTuple lavTuple = new LavTuple();
        lavTuple.minuteNumber = (short) (m + 1);
        lavTuple.xway = segmentIdentifier.xway;
        lavTuple.segment = segmentIdentifier.segment;
        lavTuple.direction = segmentIdentifier.direction;
        lavTuple.lav = lav;

        collector.collect(new Tuple2<>(timestamp, lavTuple)); // TODO msgId, SYSStamp apparently not used
    }

    private Integer computeLavValue(List<Integer> latestAvgSpeeds) {
        int speedSum = 0;
        int valueCount = 0;
        for (Integer speed : latestAvgSpeeds) {
            speedSum += speed;
            ++valueCount;
            if (valueCount > 10) {//workaround to ensure constant workload.
                break;
            }
        }

        return speedSum / valueCount;
    }
}
