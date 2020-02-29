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

class ECR extends BaseRichBolt {
    private static final Logger LOG = Log.get(ECR.class);

    private OutputCollector outputCollector;

    private ODTDBloomFilter filter;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;

        int numElements = Constants.ECR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ECR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ECR_BUCKETS_PER_WORD;
        double beta = Constants.ECR_BETA;
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

        if (cdr.callEstablished) {
            String caller = cdr.callingNumber;
            // add numbers to filters
            filter.add(caller, 1, cdr.answerTimestamp);
            double ecr = filter.estimateCount(caller, cdr.answerTimestamp);

            Values out = new Values(ScorerMap.ECR, timestamp, caller, cdr.answerTimestamp, ecr, cdr);
            outputCollector.emit(out);
            LOG.debug("tuple out: {}", out);
        }

        //outputCollector.ack(tuple);
    }
}
