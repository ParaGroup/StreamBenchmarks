package YSB;

import Util.Log;
import Util.Sampler;
import java.util.Map;
import Util.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Constants.YSBConstants.Field;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

// class ConsoleSink
public class DroppedSink extends RichSinkFunction<Joined_Event> {
    private static final Logger LOG = Log.get(DroppedSink.class);
    private long count;

    // contructor
    DroppedSink() {
        count = 0;
    }

    // invoke method
    @Override
    public void invoke(Joined_Event input, Context context) throws Exception {
        count++;
    }

    // close method
    @Override
    public void close() {
        LOG.info("[DoppedSink] dropped tuples: " + count);
    }
}
