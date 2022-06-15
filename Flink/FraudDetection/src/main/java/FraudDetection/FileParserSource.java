/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package FraudDetection;

import Util.Log;
import Util.Sampler;
import java.io.File;
import java.util.Map;
import Util.MetricGroup;
import org.slf4j.Logger;
import java.util.Scanner;
import java.io.IOException;
import java.util.ArrayList;
import Util.ThroughputCounter;
import org.slf4j.LoggerFactory;
import java.io.FileNotFoundException;
import static Constants.BaseConstants.*;
import Constants.FraudDetectionConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The source is in charge of reading the input data file, parsing it
 *  and generating the stream of records toward the FraudPredictorFilter.
 *  
 *  Format of the input file:
 *  <entityID, transaction, transactionType>
 */ 
public class FileParserSource extends RichParallelSourceFunction<Source_Event> {
    private static final Logger LOG = Log.get(FileParserSource.class);
    private String file_path;
    private String split_regex;
    private Integer rate;
    private long generated;
    private long nt_execution;
    private int par_deg;
    private int index;
    private long runTimeSec;
    private Sampler throughput;
    private boolean isRunning;
    // state of the spout (contains parsed data)
    private ArrayList<String> entities;
    private ArrayList<String> records;

    // Constructor
    public FileParserSource(String file, String split, int gen_rate, int p_deg, long _runTimeSec) {
        file_path = file;
        split_regex = split;
        rate = gen_rate;        // number of tuples per second
        par_deg = p_deg;        // spout parallelism degree
        generated = 0;          // total number of generated tuples
        nt_execution = 0;       // number of executions of nextTuple() method
        index = 0;
        runTimeSec = _runTimeSec;
        isRunning = true;
        entities = new ArrayList<>();
        records = new ArrayList<>();
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
    public void run(SourceContext<Source_Event> ctx) throws Exception {
        long epoch = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - epoch < runTimeSec * 1e9) && isRunning) {
            // send tuple
            long timestamp = System.nanoTime();
            ctx.collect(new Source_Event(entities.get(index), records.get(index), timestamp));
            generated++;
            index++;
            if (rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            // check the dataset boundaries
            if (index >= entities.size()) {
                index = 0;
                nt_execution++;
            }
        }
        // terminate the generation
        isRunning = false;
        // dump metric
        double rate = generated / ((System.nanoTime() - epoch) / 1e9); // per second
        long t_elapsed = (long) ((System.nanoTime() - epoch) / 1e6);  // elapsed time in milliseconds
        /*LOG.info("[Source] execution time: " + t_elapsed +
                 " ms, generations: " + nt_execution +
                 ", generated: " + generated +
                 ", bandwidth: " + rate +  // tuples per second
                 " tuples/s");*/
        //throughput.add(rate);
        //MetricGroup.add("throughput", throughput);
        ThroughputCounter.add(generated);
    }

    // parseDataset method
    private void parseDataset() {
        try {
            Scanner scan = new Scanner(new File(file_path));
            while (scan.hasNextLine()) {
                String[] line = scan.nextLine().split(split_regex, 2);
                entities.add(line[0]);
                records.add(line[1]);
                //LOG.debug("[Source] tuple: entityID " + line[0] + ", record " + line[1]);
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
