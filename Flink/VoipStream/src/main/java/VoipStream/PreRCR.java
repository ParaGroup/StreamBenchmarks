package VoipStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

public class PreRCR extends RichFlatMapFunction<
        Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double>,
        Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> {
    private static final Logger LOG = Log.get(PreRCR.class);

    @Override
    public void flatMap(Tuple7<Long, String, String, Long, Boolean, CallDetailRecord, Double> tuple, Collector<Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double>> collector) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = tuple.f0;

        // fetch values from the tuple
        String callingNumber = tuple.f1;
        String calledNumber = tuple.f2;
        long answerTimestamp = tuple.f3;
        boolean newCallee = tuple.f4;
        CallDetailRecord cdr = tuple.f5;

        // emits the tuples twice, the key is calling then called number
        Tuple8<String, Long, String, String, Long, Boolean, CallDetailRecord, Double> output = new Tuple8<>(null, timestamp, callingNumber, calledNumber, answerTimestamp, newCallee, cdr, tuple.f6);

        output.f0 = callingNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);


        output.f0 = calledNumber;
        collector.collect(output);
        LOG.debug("tuple out: {}", output);
    }
}
