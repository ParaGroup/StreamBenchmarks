package WordCount;

import Util.Log;
import Util.Sampler;
import java.util.Map;
import org.slf4j.Logger;
import Util.MetricGroup;
import org.slf4j.LoggerFactory;
import Util.config.Configuration;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import Constants.WordCountConstants.Field;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/** 
 *  @author  Alessandra Fais
 *  @version July 2019
 *  
 *  Sink node that receives and prints the results.
 */ 
public class ConsoleSink extends BaseRichBolt {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    private int gen_rate;
    private Sampler latency;
    private final long samplingRate;
    private long bytes;
    private long words;

    // constructor
    ConsoleSink(int p_deg, int g_rate, long _samplingRate) {
        par_deg = p_deg;         // sink parallelism degree
        gen_rate = g_rate;       // generation rate of the source (spout)
        samplingRate = _samplingRate;
    }

    // prepare method
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        bytes = 0;                   // total number of processed bytes
        words = 0;                   // total number of processed words
        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;
        latency = new Sampler(samplingRate);
    }

    // execute method
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(Field.WORD);
        long count = tuple.getLongByField(Field.COUNT);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)(now - timestamp) / 1e3, now);
        //collector.ack(tuple);
        processed++;
        bytes += word.getBytes().length;
        words++;
        t_end = System.nanoTime();
    }

    // cleanup method
    @Override
    public void cleanup() {
        if (processed == 0) {
            //LOG.info("[Sink] processed tuples: " + processed);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
            String formatted_mbs = String.format("%.5f", mbs);
            /*LOG.info("[Sink] processed: " +
                        words + " (words) " +
                        (bytes / 1048576) + " (MB), " +
                     "bandwidth: " +
                        words / (t_elapsed / 1000) + " (words/s) " +
                        formatted_mbs + " (MB/s) " +
                        bytes / (t_elapsed / 1000) + " (bytes/s).");*/
            MetricGroup.add("latency", latency);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.WORD, Field.COUNT, Field.TIMESTAMP));
    }
}
