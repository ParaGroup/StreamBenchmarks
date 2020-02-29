package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

import java.util.Map;

class PreRCR extends BaseRichBolt {
    private static final Logger LOG = Log.get(PreRCR.class);

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "timestamp", "calling_number", "called_number", "answer_timestamp", "new_callee", "cdr", "ecr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");

        // fetch values from the tuple
        String callingNumber = (String) tuple.getValueByField("calling_number");
        String calledNumber = (String) tuple.getValueByField("called_number");
        long answerTimestamp = (long) tuple.getValueByField("answer_timestamp");
        boolean newCallee = (Boolean) tuple.getValueByField("new_callee");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        double ecr = (double) tuple.getValueByField("ecr");

        // emits the tuples twice, the key is calling then called number
        Values values = new Values(callingNumber, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, ecr);
        outputCollector.emit(values);
        LOG.debug("tuple out: {}", values);

        values = new Values(calledNumber, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, ecr);
        outputCollector.emit(values);
        LOG.debug("tuple out: {}", values);

        //outputCollector.ack(tuple);
    }
}
