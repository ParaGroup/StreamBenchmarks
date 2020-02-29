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
public class ConsoleSink extends RichSinkFunction<Aggregate_Event> {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    private int gen_rate;
    private Sampler latency;
    private final long samplingRate;

    // contructor
    ConsoleSink(int p_deg, int g_rate, long _samplingRate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
        samplingRate = _samplingRate;
        processed = 0;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        latency = new Sampler(samplingRate);
    }

    // invoke method
    @Override
    public void invoke(Aggregate_Event input, Context context) throws Exception {
        String cmp_id = input.cmp_id;
        long count = input.count;
        long timestamp = input.timestamp;
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)(now - timestamp) / 1e3, now);
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() {
        if (processed == 0) {
            //LOG.info("[Sink] processed tuples: " + processed);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            /*LOG.info("[Sink] execution time: " + t_elapsed +
                    " ms, processed: " + processed +
                    ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                    " tuples/s");*/
            MetricGroup.add("latency", latency);
        }
    }
}
