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
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

import java.util.Map;

class CT24 extends BaseRichBolt {
    private static final Logger LOG = Log.get(CT24.class);

    private OutputCollector outputCollector;

    private ODTDBloomFilter filter;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // initialize
        this.outputCollector = outputCollector;

        int numElements = Constants.CT24_NUM_ELEMENTS;
        int bucketsPerElement = Constants.CT24_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.CT24_BUCKETS_PER_WORD;
        double beta = Constants.CT24_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "calling_number", "answer_timestamp", "rate", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");

        boolean newCallee = tuple.getBooleanByField("new_callee");
        if (cdr.callEstablished && newCallee) {
            String caller = tuple.getStringByField("calling_number");
            filter.add(caller, cdr.callDuration, cdr.answerTimestamp);
            double callTime = filter.estimateCount(caller, cdr.answerTimestamp);

            Values out = new Values(ScorerMap.CT24, timestamp, caller, cdr.answerTimestamp, callTime, cdr);
            outputCollector.emit(out);
            LOG.debug("tuple out: {}", out);
        }

        //outputCollector.ack(tuple);
    }
}
