package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import util.Log;

import java.util.Map;

public class Score extends RichMapFunction<
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>,
        Tuple5<Long, String, Long, Double, CallDetailRecord>> {
    private static final Logger LOG = Log.get(Score.class);

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
    public void open(Configuration parameters) {

        scorerMap = new ScorerMap(new int[]{ScorerMap.FoFiR, ScorerMap.URL, ScorerMap.ACD});

        weights = new double[3];
        weights[0] = Constants.FOFIR_WEIGHT;
        weights[1] = Constants.URL_WEIGHT;
        weights[2] = Constants.ACD_WEIGHT;
    }

    @Override
    public Tuple5<Long, String, Long, Double, CallDetailRecord> map(Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        int source = tuple.f0;

        CallDetailRecord cdr = tuple.f5;
        String number = tuple.f2;
        long answerTimestamp = tuple.f3;
        double score = tuple.f4;

        String key = String.format("%s:%d", number, answerTimestamp);

        Map<String, ScorerMap.Entry> map = scorerMap.getMap();
        if (map.containsKey(key)) {
            ScorerMap.Entry entry = map.get(key);

            if (entry.isFull()) {
                double mainScore = sum(entry.getValues(), weights);

                Tuple5<Long, String, Long, Double, CallDetailRecord> out = new Tuple5<>(timestamp, number, answerTimestamp, mainScore, cdr);
                LOG.debug("tuple out: {}", out);
                return out;
            } else {
                entry.set(source, score);

                Tuple5<Long, String, Long, Double, CallDetailRecord> out = new Tuple5<>(timestamp, number, answerTimestamp, 0d, cdr);
                LOG.debug("tuple out: {}", out);
                return out;
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, score);
            map.put(key, entry);

            Tuple5<Long, String, Long, Double, CallDetailRecord> out = new Tuple5<>(timestamp, number, answerTimestamp, 0d, cdr);
            LOG.debug("tuple out: {}", out);
            return out;
        }
    }
}
