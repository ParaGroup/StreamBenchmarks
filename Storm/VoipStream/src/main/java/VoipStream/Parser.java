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

class Parser extends BaseRichBolt {
    private static final Logger LOG = Log.get(Parser.class);

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "calling_number", "called_number", "answer_timestamp", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        // fetch values from the tuple
        long timestamp = (long) tuple.getValueByField("timestamp");
        String line = (String) tuple.getValueByField("line");

        // parse the line
        CallDetailRecord cdr = new CallDetailRecord(line);

        Values out = new Values(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, cdr);
        outputCollector.emit(out);
        LOG.debug("tuple out: {}", out);

        //outputCollector.ack(tuple);
    }
}
