package WordCount;

import Util.Log;
import Constants.WordCountConstants.Field;
import Util.config.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  Counts words' occurrences.
 */
public class CounterBolt extends BaseRichBolt {
    private static final Logger LOG = Log.get(CounterBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private final Map<String, MutableLong> counts = new HashMap<>();

    private long t_start;
    private long t_end;
    private int par_deg;
    private long bytes;
    private long words;

    CounterBolt(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        bytes = 0;                   // total number of processed bytes
        words = 0;                   // total number of processed words

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(Field.WORD);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        if (word != null) {
            bytes += word.getBytes().length;
            words++;
            MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
            count.increment();
            collector.emit(new Values(word, count.get(), timestamp));
        }
        //collector.ack(tuple);
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);

        /*LOG.info("[Counter] execution time: " + t_elapsed + " ms, " +
                "processed: " + words + " (words) " + (bytes / 1048576) + " (MB), " +
                "bandwidth: " + words / (t_elapsed / 1000) + " (words/s) "
                              + formatted_mbs + " (MB/s) "
                              + bytes / (t_elapsed / 1000) + " (bytes/s)");*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.WORD, Field.COUNT, Field.TIMESTAMP));
    }
}
