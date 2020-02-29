package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.BloomFilter;
import common.CallDetailRecord;
import common.Constants;
import util.Log;

public class Dispatcher extends RichMapFunction<
        Tuple5<Long, String, String, Long, CallDetailRecord>,
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(Dispatcher.class);

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private double cycleThreshold;

    @Override
    public void open(Configuration parameters) {

        int approxInsertSize = Constants.VAR_DETECT_APROX_SIZE;
        double falsePostiveRate = Constants.VAR_DETECT_ERROR_RATE;
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        cycleThreshold = detector.size() / Math.sqrt(2);
    }

    @Override
    public Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> map(Tuple5<Long, String, String, Long, CallDetailRecord> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;

        // fetch values from the tuple
        CallDetailRecord cdr = tuple.f4;
        String key = String.format("%s:%s", cdr.callingNumber, cdr.calledNumber);
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> out = new Tuple7<>(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, newCallee, cdr, -1.0);
        LOG.debug("tuple out: {}", out);
        return out;
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}
