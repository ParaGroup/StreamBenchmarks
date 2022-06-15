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
import Constants.FraudDetectionConstants.*;
import MarkovModelPrediction.MarkovModelPredictor;
import MarkovModelPrediction.ModelBasedPredictor;
import MarkovModelPrediction.Prediction;
import Util.config.Configuration;
import org.apache.commons.lang.StringUtils;
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
 *  The bolt is in charge of implementing outliers detection.
 *  Given a transaction sequence of a customer, there is a probability associated with each path
 *  of state transition which indicates the chances of fraudolent activities. Only tuples for
 *  which an outlier has been identified are sent out.
 */
public class FraudPredictorBolt extends BaseRichBolt {
    private static final Logger LOG = Log.get(FraudPredictorBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private ModelBasedPredictor predictor;
    private long t_start;
    private long t_end;
    private long processed;
    private long outliers;
    private int par_deg;

    FraudPredictorBolt(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        outliers = 0;                // total number of outliers

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;

        String strategy = config.getString(Conf.PREDICTOR_MODEL);
        if (strategy.equals("mm")) {
            LOG.debug("[Predictor] creating Markov Model Predictor");
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String entityID = tuple.getStringByField(Field.ENTITY_ID);
        String record = tuple.getStringByField(Field.RECORD_DATA);
        Long timestamp = tuple.getLongByField(Field.TIMESTAMP);

        Prediction p = predictor.execute(entityID, record);
        LOG.debug("[Predictor] tuple: entityID " + entityID + ", record " + record + ", ts " + timestamp);

        // send outliers
        if (p.isOutlier()) {
            outliers++;
            collector.emit(tuple,
                    new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), timestamp));

            LOG.debug("[Predictor] outlier: entityID " + entityID + ", score " + p.getScore() +
                            ", states " + StringUtils.join(p.getStates(), ","));
        }
        //collector.ack(tuple);

        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        /*LOG.info("[Predictor] execution time: " + t_elapsed +
                " ms, processed: " + processed +
                ", outliers: " + outliers +
                ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.ENTITY_ID, Field.SCORE, Field.STATES, Field.TIMESTAMP));
    }
}
