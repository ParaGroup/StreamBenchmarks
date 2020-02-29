package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import util.Log;

import java.util.Map;

public class FoFiR_reordering extends RichFlatMapFunction<
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>,
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> {
    private static final Logger LOG = Log.get(FoFiR.class);

    private ScorerMap scorerMap;

    @Override
    public void open(Configuration parameters) {

        scorerMap = new ScorerMap(new int[]{ScorerMap.RCR, ScorerMap.ECR});
    }

    @Override
    public void flatMap(Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double> tuple, Collector<Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f1;
        int source = tuple.f0;

        CallDetailRecord cdr = tuple.f5;
        String number = tuple.f2;
        long answerTimestamp = tuple.f3;
        double rate = tuple.f4;

        String key = String.format("%s:%d", number, answerTimestamp);

        Map<String, ScorerMap.Entry> map = scorerMap.getMap();
        if (map.containsKey(key)) {
            ScorerMap.Entry entry = map.get(key);
            entry.set(source, rate);

            // add ECR if present
            if (tuple.f6 >= 0) {
                entry.set(ScorerMap.ECR, tuple.f6);
            }

            if (entry.isFull()) {
                // calculate the score for the ratio
                double ratio = (entry.get(ScorerMap.ECR) / entry.get(ScorerMap.RCR));
                double score = ScorerMap.score(Constants.FOFIR_THRESHOLD_MIN, Constants.FOFIR_THRESHOLD_MAX, ratio);

                Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> out = new Tuple6<>(ScorerMap.FoFiR, timestamp, number, answerTimestamp, score, cdr);
                collector.collect(out);
                LOG.debug("tuple out: {}", out);

                map.remove(key);
            }
        } else {
            ScorerMap.Entry entry = scorerMap.newEntry();
            entry.set(source, rate);
            map.put(key, entry);
        }
    }
}
