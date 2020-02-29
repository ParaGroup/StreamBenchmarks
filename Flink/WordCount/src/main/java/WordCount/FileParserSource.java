package WordCount;

import Util.Log;
import Util.Sampler;
import java.io.File;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import Util.ThroughputCounter;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import static Constants.BaseConstants.*;
import Constants.WordCountConstants.Conf;
import Constants.WordCountConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The spout is in charge of reading the input data file containing
 *  words, parsing it and generating a stream of lines toward the
 *  SplitterBolt. In alternative it generates a stream of random
 *  sentences.
 */ 
public class FileParserSource extends RichParallelSourceFunction<Sentence_Event> {
    private static final Logger LOG = Log.get(FileParserSource.class);
    private String file_path;
    private Integer rate;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long runTimeSec;
    private Sampler throughput;
    private boolean isRunning;
    private ArrayList<String> lines;    // state of the spout (contains parsed data)
    private long bytes;                 // accumulates the amount of bytes emitted

    // Constructor
    public FileParserSource(String file, int gen_rate, int p_deg, long _runTimeSec) {
        file_path = file;
        rate = gen_rate;            // number of tuples per second
        par_deg = p_deg;            // spout parallelism degree
        generated = 0;              // total number of generated tuples
        nt_execution = 0;           // number of executions of nextTuple() method
        index = 0;
        runTimeSec = _runTimeSec;
        isRunning = true;
        lines = new ArrayList<>();
        bytes = 0;                  // total number of bytes emitted
        // save tuples as a state
        parseDataset();
    }

    // open method
    @Override
    public void open(Configuration parameters) throws IOException {
        throughput = new Sampler();
    }

    // run method
    @Override
    public void run(SourceContext<Sentence_Event> ctx) throws Exception {
        long epoch = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - epoch < runTimeSec * 1e9) && isRunning) {
            // send tuple
            long timestamp = System.nanoTime();
            ctx.collect(new Sentence_Event(lines.get(index), timestamp));
            bytes += lines.get(index).getBytes().length;
            generated++;
            index++;
            if (rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            // check the dataset boundaries
            if (index >= lines.size()) {
                index = 0;
                nt_execution++;
            }
        }
        // terminate the generation
        isRunning = false;
        // dump metric
        double rate = generated / ((System.nanoTime() - epoch) / 1e9); // per second
        long t_elapsed = (long) ((System.nanoTime() - epoch) / 1e6);  // elapsed time in milliseconds
        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);
        /*LOG.info("[Source] execution time: " + t_elapsed +
                    " ms, generations: " + nt_execution +
                    ", generated: " + generated + " (lines) " + (bytes / 1048576) +
                    " (MB), bandwidth: " + generated / (t_elapsed / 1000) +
                    " (lines/s) " + formatted_mbs + " (MB/s)");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        ThroughputCounter.add(generated, bytes);
    }

    // parseDataset method
    private void parseDataset() {
        try {
            Scanner scan = new Scanner(new File(file_path));
            while (scan.hasNextLine()) {
                lines.add(scan.nextLine());
            }
            scan.close();
        }
        catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", file_path);
            throw new RuntimeException("The file '" + file_path + "' does not exists");
        }
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }

    @Override
    public void cancel() {
        //LOG.info("cancel");
        isRunning = false;
    }

    @Override
    public void close() {
        //LOG.info("close");
    }
}
