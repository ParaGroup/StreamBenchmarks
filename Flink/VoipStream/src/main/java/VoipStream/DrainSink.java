package VoipStream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;

public class DrainSink<T extends Tuple> extends RichSinkFunction<T> {
    private static final Logger LOG = Log.get(DrainSink.class);

    private final long samplingRate;

    private Sampler latency;

    public DrainSink(long samplingRate) {
        this.samplingRate = samplingRate;
    }

    @Override
    public void open(Configuration parameters) {
        latency = new Sampler(samplingRate);
    }

    @Override
    public void invoke(Tuple tuple, Context context) {
        LOG.debug("tuple in: {}", tuple);

        assert tuple.getArity() > 1;

        long timestamp = tuple.getField(0);
        long now = System.nanoTime();
        latency.add((now - timestamp) / 1e3, now); // microseconds
    }

    @Override
    public void close() {
        MetricGroup.add("latency", latency);
    }
}
