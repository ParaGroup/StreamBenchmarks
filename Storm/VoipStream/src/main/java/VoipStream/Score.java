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

class Score extends BaseRichBolt {
    private static final Logger LOG = Log.get(Score.class);

    private OutputCollector outputCollector;

    private ScorerMap scorerMap;
    private double[] weights;

    /**
     * Computes weighted sum of a given sequence.
     *
     * @param data    data array
     * @param weights weights
     * @return weighted sum of the data
     */
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i = 0; i < data.length; i++) {
            sum += (data[i] * weights[i]);
        }

        return sum;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
        scorerMap = new ScorerMap(new int[]{ScorerMap.FoFiR, ScorerMap.URL, ScorerMap.ACD});

        weights = new double[3];
        weights[0] = Constants.FOFIR_WEIGHT;
        weights[1] = Constants.URL_WEIGHT;
        weights[2] = Constants.ACD_WEIGHT;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "calling_number", "answer_timestamp", "score", "cdr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        int source = (int) tuple.getValueByField("source");

        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        String number = (String) tuple.getValueByField("calling_number");
        long answerTimestamp = (long) tuple.getValueByField("answer_timestamp");
        double score = (double) tuple.getValueByField("score");

        String key = String.format("%s:%d", number, answerTimestamp);

        Map<String, ScorerMap.Entry> map = scorerMap.getMap();
        if (map.containsKey(key)) {
            ScorerMap.Entry entry = map.get(key);

            if (entry.isFull()) {
                double mainScore = sum(entry.getValues(), weights);

                Values out = new Values(timestamp, number, answerTimestamp, mainScore, cdr);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);
            } else {
                entry.set(source, score);

                Values out = new Values(timestamp, number, answerTimestamp, 0.0, cdr);
                outputCollector.emit(out);
                LOG.debug("tuple out: {}", out);
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, score);
            map.put(key, entry);

            Values out = new Values(timestamp, number, answerTimestamp, 0.0, cdr);
            outputCollector.emit(out);
            LOG.debug("tuple out: {}", out);
        }

        //outputCollector.ack(tuple);
    }
}
