package LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.AvgVehicleSpeedTuple;
import common.LavTuple;
import common.SegmentIdentifier;
import util.Log;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

class LastAverageSpeed extends BaseRichBolt {
    private static final Logger LOG = Log.get(LastAverageSpeed.class);

    private OutputCollector outputCollector;

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
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "lav"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("execute");

        long timestamp = (long) tuple.getValueByField("timestamp");

        //////////

        // TODO apparently not used downstream...
//        Long msgId;
//        Long SYSStamp;
//        msgId = input.getLongByField(MSG_ID);
//        SYSStamp = input.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

        AvgVehicleSpeedTuple inputTuple = (AvgVehicleSpeedTuple) tuple.getValueByField("average_speed");

        Short minuteNumber = inputTuple.minute;
        short m = minuteNumber;

        this.segmentIdentifier.xway = inputTuple.xway;
        this.segmentIdentifier.segment = inputTuple.segment;
        this.segmentIdentifier.direction = inputTuple.direction;

        List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
        List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);

        if (latestAvgSpeeds == null) {
            latestAvgSpeeds = new LinkedList<>();
            try {
                this.averageSpeedsPerSegment.put((SegmentIdentifier) this.segmentIdentifier.clone(), latestAvgSpeeds);
            } catch (CloneNotSupportedException e) {
                outputCollector.reportError(e);
            }
            latestMinuteNumber = new LinkedList<>();
            try {
                this.minuteNumbersPerSegment.put((SegmentIdentifier) this.segmentIdentifier.clone(), latestMinuteNumber);
            } catch (CloneNotSupportedException e) {
                outputCollector.reportError(e);
            }
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

        this.outputCollector.emit(new Values(timestamp, lavTuple)); // TODO msgId, SYSStamp apparently not used
        //////////

        //outputCollector.ack(tuple);
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
