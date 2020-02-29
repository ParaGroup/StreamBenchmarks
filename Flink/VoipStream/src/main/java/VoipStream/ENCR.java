package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

public class ENCR extends RichFlatMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(ENCR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.ENCR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ENCR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ENCR_BUCKETS_PER_WORD;
        double beta = Constants.ENCR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple, Collector<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        CallDetailRecord cdr = tuple.f5;

        boolean newCallee = tuple.f4;

        if (cdr.callEstablished && newCallee) {
            String caller = tuple.f1;
            filter.add(caller, 1, cdr.answerTimestamp);
            double rate = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double> out = new Tuple7<>(ScorerMap.ENCR, timestamp, caller, cdr.answerTimestamp, rate, cdr, tuple.f6);
            collector.collect(out);
            LOG.debug("tuple out: {}", out);
        }
    }
}
