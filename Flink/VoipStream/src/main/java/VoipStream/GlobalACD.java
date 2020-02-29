package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import common.CallDetailRecord;
import common.Constants;
import common.ScorerMap;
import common.VariableEWMA;
import util.Log;

// XXX include some common fields to CT24 and ECR24 since Flink tuples must be strongly typed
public class GlobalACD extends RichMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord>> {
    private static final Logger LOG = Log.get(GlobalACD.class);

    private VariableEWMA avgCallDuration;

    @Override
    public void open(Configuration parameters) {

        double decayFactor = Constants.ACD_DECAY_FACTOR;
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> map(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;
        CallDetailRecord cdr = tuple.f5;
        avgCallDuration.add(cdr.callDuration);

        Tuple6<Integer, Long, String, Long, Double, CallDetailRecord> out = new Tuple6<>(ScorerMap.GlobalACD, timestamp, cdr.callingNumber, cdr.answerTimestamp, avgCallDuration.getAverage(), cdr);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
