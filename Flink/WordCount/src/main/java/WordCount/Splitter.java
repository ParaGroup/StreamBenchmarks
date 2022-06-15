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

package WordCount;

import Util.Log;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import Constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  Splits all the received lines into words.
 */ 
public class Splitter extends RichFlatMapFunction<Sentence_Event, Word_Event> {
    private static final Logger LOG = Log.get(FileParserSource.class);
    private long t_start;
    private long t_end;
    private int par_deg;
    private long bytes;
    private long line_count;
    private long word_count;

    // Constructor
    public Splitter(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        bytes = 0;                   // total number of processed bytes
        line_count = 0;              // total number of processed input tuples (lines)
        word_count = 0;              // total number of emitted tuples (words)
    }

    // flatmap method
    @Override
    public void flatMap(Sentence_Event input, Collector<Word_Event> output) {
        String line = input.sentence;
        long timestamp = input.ts;
        if (line != null) {
            bytes += line.getBytes().length;
            line_count++;
            String[] words = line.split(" ");
            for (String word : words) {
                //if (!StringUtils.isBlank(word)) {
                    output.collect(new Word_Event(word, timestamp));
                    word_count++;
                //}
            }
        }
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        double mbs = (double)(bytes / 1048576) / (double)(t_elapsed / 1000);
        String formatted_mbs = String.format("%.5f", mbs);
        /*LOG.info("[Splitter] execution time: " + t_elapsed + " ms, " +
                            "processed: " + line_count + " (lines) "
                            + word_count + " (words) "
                            + (bytes / 1048576) + " (MB), " +
                            "bandwidth: " + word_count / (t_elapsed / 1000) + " (words/s) "
                            + formatted_mbs + " (MB/s) "
                            + bytes / (t_elapsed / 1000) + " (bytes/s)");*/
    }
}
