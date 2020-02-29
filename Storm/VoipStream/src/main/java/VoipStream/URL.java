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
import common.ScorerMap;
import util.Log;

import java.util.Map;

class URL extends BaseRichBolt {
    private static final Logger LOG = Log.get(URL.class);

    private OutputCollector outputCollector;

    private ScorerMap scorerMap;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
        scorerMap = new ScorerMap(new int[]{ScorerMap.ENCR, ScorerMap.ECR});
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "timestamp", "calling_number", "answer_timestamp", "score", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        int source = (int) tuple.getValueByField("source");

        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        String number = (String) tuple.getValueByField("calling_number");
        long answerTimestamp = (long) tuple.getValueByField("answer_timestamp");
        double rate = (double) tuple.getValueByField("rate");

        String key = String.format("%s:%d", number, answerTimestamp);

        Map<String, ScorerMap.Entry> map = scorerMap.getMap();
        if (map.containsKey(key)) {
            ScorerMap.Entry entry = map.get(key);
            entry.set(source, rate);

            if (entry.isFull()) {
                // calculate the score for the ratio
                double ratio = (entry.get(ScorerMap.ENCR) / entry.get(ScorerMap.ECR));
                double score = ScorerMap.score(Constants.URL_THRESHOLD_MIN, Constants.URL_THRESHOLD_MAX, ratio);

                Values out = new Values(ScorerMap.URL, timestamp, number, answerTimestamp, score, cdr);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);

                map.remove(key);
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, rate);
            map.put(key, entry);
        }

        //outputCollector.ack(tuple);
    }
}
