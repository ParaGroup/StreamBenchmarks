package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ODTDBloomFilter;
import common.ScorerMap;
import util.Log;

public class ECR extends RichFlatMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(ECR.class);

    private ODTDBloomFilter filter;

    @Override
    public void open(Configuration parameters) {

        int numElements = Constants.ECR_NUM_ELEMENTS;
        int bucketsPerElement = Constants.ECR_BUCKETS_PER_ELEMENT;
        int bucketsPerWord = Constants.ECR_BUCKETS_PER_WORD;
        double beta = Constants.ECR_BETA;
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple, Collector<Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        CallDetailRecord cdr = tuple.f5;

        if (cdr.callEstablished) {
            String caller = cdr.callingNumber;
            // add numbers to filters
            filter.add(caller, 1, cdr.answerTimestamp);
            double ecr = filter.estimateCount(caller, cdr.answerTimestamp);

            Tuple7<Integer, Long, String, Long, Double, CallDetailRecord, Double> out = new Tuple7<>(ScorerMap.ECR, timestamp, caller, cdr.answerTimestamp, ecr, cdr, tuple.f6);
            collector.collect(out);
            LOG.debug("tuple out: {}", out);
        }
    }
}
