package WordCount;

import Util.Log;
import Util.Sampler;
import java.util.Map;
import Util.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import Constants.WordCountConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  Sink node that receives and prints the results.
 */ 
public class ConsoleSink extends RichSinkFunction<Count_Event> {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long t_start;
    private long t_end;
    private long processed;
    private long bytes;
    private long words;
    private int par_deg;
    private int gen_rate;
    private Sampler latency;
    private final long samplingRate;

    // Constructor
    public ConsoleSink(int p_deg, int g_rate, long _samplingRate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
        samplingRate = _samplingRate;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;
        bytes = 0;                   // total number of processed bytes
        words = 0;                   // total number of processed words
        latency = new Sampler(samplingRate);
    }

    // invoke method
    @Override
    public void invoke(Count_Event input, Context context) throws Exception {
        String word = input.word;
        long count = input.count;
        long timestamp = input.ts;
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)(now - timestamp) / 1e3, now);
        processed++;
        bytes += word.getBytes().length;
        words++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() {
        if (processed == 0) {
            //LOG.info("[Sink] processed tuples: " + processed);
        }
        else {
            // evaluate bandwidth and latency
            long t_elapsed = (t_end - t_start) / 1000000;       // elapsed time in ms
            double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
            String formatted_mbs = String.format("%.5f", mbs);
            // bandwidth summary
            /*LOG.info("[Sink] processed: " +
                                words + " (words) " +
                                (bytes / 1048576) + " (MB), " +
                                "bandwidth: " +
                                words / (t_elapsed / 1000) + " (words/s) " +
                                formatted_mbs + " (MB/s) " +
                                bytes / (t_elapsed / 1000) + " (bytes/s)");*/
            MetricGroup.add("latency", latency);
        }
    }
}
