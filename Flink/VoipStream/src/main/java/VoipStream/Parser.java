package VoipStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.slf4j.Logger;
import common.CallDetailRecord;
import util.Log;

public class Parser extends RichMapFunction<
        Tuple2<Long, String>,
        Tuple5<Long, String, String, Long, CallDetailRecord>> {
    private static final Logger LOG = Log.get(Parser.class);

    @Override
    public Tuple5<Long, String, String, Long, CallDetailRecord> map(Tuple2<Long, String> tuple) {
        LOG.debug("tuple in: {}", tuple);

        // fetch values from the tuple
        long timestamp = tuple.f0;
        String line = tuple.f1;

        // parse the line
        CallDetailRecord cdr = new CallDetailRecord(line);

        Tuple5<Long, String, String, Long, CallDetailRecord> out = new Tuple5<>(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, cdr);
        LOG.debug("tuple out: {}", out);
        return out;
    }
}
