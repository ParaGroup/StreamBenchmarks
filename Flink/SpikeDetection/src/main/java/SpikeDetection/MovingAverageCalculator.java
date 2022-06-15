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
import java.util.HashMap;
import java.util.LinkedList;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import Constants.SpikeDetectionConstants.Conf;
import Constants.SpikeDetectionConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The bolt is in charge of computing the average over a window of values.
 *  It manages one window for each device_id.
 *  
 *  See http://github.com/surajwaghulde/storm-example-projects
 */ 
public class MovingAverageCalculator extends RichFlatMapFunction<Source_Event, Output_Event> {
    private static final Logger LOG = Log.get(MovingAverageCalculator.class);
    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    private Configuration config;

    // Constructor
    public MovingAverageCalculator(int p_deg, Configuration _config) {
        par_deg = p_deg;     // bolt parallelism degree
        config = _config;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        movingAverageWindow = config.getInteger(Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    // flatmap method
    @Override
    public void flatMap(Source_Event input, Collector<Output_Event> output) {
        String deviceID = input.deviceID;
        double next_property_value = input.value;
        long timestamp = input.ts;
        double moving_avg_instant = movingAverage(deviceID, next_property_value);
        output.collect(new Output_Event(deviceID, moving_avg_instant, next_property_value, timestamp));
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[Average] execution time: " + t_elapsed +
                            " ms, processed: " + processed +
                            ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");*/
    }

    // movingAverage method
    private double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<>();
        double sum = 0.0;
        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow - 1) {
                double valueToRemove = valueList.removeFirst();
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum / valueList.size();
        }
        else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }
}
