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

package SpikeDetection;

import Util.Log;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import Constants.SpikeDetectionConstants;
import Constants.SpikeDetectionConstants.Conf;
import Constants.SpikeDetectionConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The bolt is in charge of detecting spikes in the measurements received by sensors
 *  with respect to a properly defined threshold.
 */ 
public class SpikeDetector extends RichFlatMapFunction<Output_Event, Output_Event> {
    private static final Logger LOG = Log.get(SpikeDetector.class);
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    private double spike_threshold;
    private long spikes;
    private Configuration config;

    // Constructor
    public SpikeDetector(int p_deg, Configuration _config) {
        par_deg = p_deg;     // bolt parallelism degree
        config = _config;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        spikes = 0;                  // total number of spikes detected
        spike_threshold = config.getDouble(Conf.SPIKE_DETECTOR_THRESHOLD, SpikeDetectionConstants.DEFAULT_THRESHOLD);
    }

    // flatmap method
    @Override
    public void flatMap(Output_Event input, Collector<Output_Event> output) {
        String deviceID = input.deviceID;
        double moving_avg_instant = input.moving_avg;
        double next_property_value = input.value;
        long timestamp = input.ts;
        if (Math.abs(next_property_value - moving_avg_instant) > spike_threshold * moving_avg_instant) {
            spikes++;
            output.collect(input);
        }
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[Detector] execution time: " + t_elapsed +
                            " ms, processed: " + processed +
                            ", spikes: " + spikes +
                            ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");*/
    }
}
