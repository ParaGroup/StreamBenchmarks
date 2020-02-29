package LinearRoad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.PositionReport;
import util.Log;

import java.util.Map;

class Dispatcher extends BaseRichBolt {
    private static final Logger LOG = Log.get(Dispatcher.class);

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "position_report"));
    }

    @Override
    public void execute(Tuple tuple) {
        // fetch values from the tuple
        long timestamp = (long) tuple.getValueByField("timestamp");
        String line = (String) tuple.getValueByField("line");
        LOG.debug("execute: {}", line);

        // parse the string
        String[] token = line.split(",");
        short type = Short.parseShort(token[0]);
        if (type == 0) { // TODO constant
            // build and emit the position report
            PositionReport positionReport = new PositionReport();
            positionReport.type = type;
            positionReport.time = Integer.parseInt(token[1]);
            positionReport.vid = Integer.parseInt(token[2]);
            positionReport.speed = Integer.parseInt(token[3]);
            positionReport.xway = Integer.parseInt(token[4]);
            positionReport.lane = Short.parseShort(token[5]);
            positionReport.direction = Short.parseShort(token[6]);
            positionReport.segment = Short.parseShort(token[7]);
            positionReport.position = Integer.parseInt(token[8]);
            outputCollector.emit(new Values(timestamp, positionReport));
        }

        //outputCollector.ack(tuple);
    }
}
