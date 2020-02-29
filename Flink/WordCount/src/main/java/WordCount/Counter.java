package WordCount;

import Util.Log;
import java.util.Map;
import org.slf4j.Logger;
import java.util.HashMap;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import Constants.WordCountConstants.Field;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  Counts words' occurrences.
 */ 
public class Counter extends RichFlatMapFunction<Word_Event, Count_Event> {
    private static final Logger LOG = Log.get(Counter.class);
    private final Map<String, MutableLong> counts = new HashMap<>();
    private long t_start;
    private long t_end;
    private int par_deg;
    private long bytes;
    private long words;

    // Constructor
    public Counter(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        bytes = 0;                   // total number of processed bytes
        words = 0;                   // total number of processed words
    }

    // flatmap method
    @Override
    public void flatMap(Word_Event input, Collector<Count_Event> output) {
        String word = input.word;
        long timestamp = input.ts;
        if (word != null) {
            bytes += word.getBytes().length;
            words++;
            MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
            count.increment();
            output.collect(new Count_Event(word, count.longValue(), timestamp));
        }
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);
        /*LOG.info("[Counter] execution time: " + t_elapsed + " ms, " +
                            "processed: " + words + " (words) " + (bytes / 1048576) + " (MB), " +
                            "bandwidth: " + words / (t_elapsed / 1000) + " (words/s) "
                            + formatted_mbs + " (MB/s) "
                            + bytes / (t_elapsed / 1000) + " (bytes/s)");*/
    }
}
