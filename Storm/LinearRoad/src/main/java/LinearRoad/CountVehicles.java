package LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.CarCount;
import common.CountTuple;
import common.PositionReport;
import common.SegmentIdentifier;
import util.Log;

import java.util.HashMap;
import java.util.Map;

class CountVehicles extends BaseRichBolt {
    private static final Logger LOG = Log.get(CountVehicles.class);

    private OutputCollector outputCollector;

    //////////
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each segment to its count value.
     */
    private final Map<SegmentIdentifier, CarCount> countsMap = new HashMap<>();
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = -1;
    //////////

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "count"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("execute");
        long timestamp = (long) tuple.getValueByField("timestamp");

        //////////
        PositionReport inputPositionReport = (PositionReport) tuple.getValueByField("position_report");

        short minute = inputPositionReport.getMinuteNumber();
        segment.xway = inputPositionReport.xway;
        segment.segment = inputPositionReport.segment;
        segment.direction = inputPositionReport.direction;

        boolean emitted = false;

        for (Map.Entry<SegmentIdentifier, CarCount> entry : this.countsMap.entrySet()) {
            SegmentIdentifier segId = entry.getKey();


            int count = entry.getValue().count;
            if (count > 50) {

                emitted = true;

                CountTuple countTuple = new CountTuple();
                countTuple.minuteNumber = currentMinute;
                countTuple.xway = segId.xway;
                countTuple.segment = segId.segment;
                countTuple.direction = segId.direction;
                countTuple.count = count;
                this.outputCollector.emit(new Values(timestamp, countTuple));
            }
        }
        if (!emitted) {
            CountTuple countTuple = new CountTuple();
            countTuple.minuteNumber = currentMinute;
            this.outputCollector.emit(new Values(timestamp, countTuple));
        }
        this.countsMap.clear();
        this.currentMinute = minute;

        CarCount segCnt = this.countsMap.get(this.segment);
        if (segCnt == null) {
            segCnt = new CarCount();
            try {
                this.countsMap.put((SegmentIdentifier) this.segment.clone(), segCnt);
            } catch (CloneNotSupportedException e) {
                outputCollector.reportError(e);
            }
        } else {
            ++segCnt.count;
        }
        //////////

        //outputCollector.ack(tuple);
    }
}
