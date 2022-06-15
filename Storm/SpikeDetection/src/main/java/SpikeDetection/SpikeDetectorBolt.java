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
import Constants.SpikeDetectionConstants;
import Constants.SpikeDetectionConstants.*;
import Util.config.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version May 2019
 *
 *  The bolt is in charge of detecting spikes in the measurements received by sensors
 *  with respect to a properly defined threshold.
 */
public class SpikeDetectorBolt extends BaseRichBolt {
    private static final Logger LOG = Log.get(SpikeDetectorBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    private double spike_threshold;
    private long spikes;

    SpikeDetectorBolt(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        //LOG.info("[Detector] started ({} replicas)", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        spikes = 0;                  // total number of spikes detected

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;

        spike_threshold = config.getDouble(Conf.SPIKE_DETECTOR_THRESHOLD, SpikeDetectionConstants.DEFAULT_THRESHOLD);
    }

    @Override
    public void execute(Tuple tuple) {
        String deviceID = tuple.getStringByField(Field.DEVICE_ID);
        double moving_avg_instant = tuple.getDoubleByField(Field.MOVING_AVG);
        double next_property_value = tuple.getDoubleByField(Field.VALUE);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);

        LOG.debug("[Detector] tuple: deviceID " + deviceID +
                    ", incremental_average " + moving_avg_instant +
                    ", next_value " + next_property_value +
                    ", ts " + timestamp);

        if (Math.abs(next_property_value - moving_avg_instant) > spike_threshold * moving_avg_instant) {
            spikes++;
            collector.emit(tuple, new Values(deviceID, moving_avg_instant, next_property_value, timestamp));
        }
        //collector.ack(tuple);

        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        /*LOG.info("[Detector] execution time: " + t_elapsed +
                " ms, processed: " + processed +
                ", spikes: " + spikes +
                ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE, Field.TIMESTAMP));
    }
}
