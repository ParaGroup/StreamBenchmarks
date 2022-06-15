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

package MarkovModelPrediction;

import Util.config.Configuration;
import Util.data.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static Constants.FraudDetectionConstants.Conf.*;
import static Constants.FraudDetectionConstants.*;

/**
 * Predictor based on markov model
 * @author pranab
 */
public class MarkovModelPredictor extends ModelBasedPredictor {
    private static final Logger LOG = LoggerFactory.getLogger(MarkovModelPredictor.class);

    private enum DetectionAlgorithm {
        MissProbability,
        MissRate,
        EntropyReduction
    };

    private MarkovModel markovModel;
    private Map<String, List<String>> records = new HashMap<>();
    private boolean localPredictor;
    private int stateSeqWindowSize;
    private int stateOrdinal;
    private DetectionAlgorithm detectionAlgorithm;
    private Map<String, Pair<Double, Double>> globalParams;
    private double metricThreshold;
    private int[] maxStateProbIndex;
    private double[] entropy;

    public MarkovModelPredictor(Configuration conf) {
        String mmKey = conf.getString(MARKOV_MODEL_KEY, null);
        String model;

        if (StringUtils.isBlank(mmKey)) {
            model = new MarkovModelResourceSource().getModel(DEFAULT_MODEL);
        } else {
            model = new MarkovModelFileSource().getModel(mmKey);
        }

        markovModel = new MarkovModel(model);
        localPredictor = conf.getBoolean(LOCAL_PREDICTOR);

        if (localPredictor) {
            stateSeqWindowSize = conf.getInt(STATE_SEQ_WIN_SIZE);
        }  else {
            stateSeqWindowSize = 5;
            globalParams = new HashMap<>();
        }

        //state value ordinal within record
        stateOrdinal = conf.getInt(STATE_ORDINAL);

        //detection algoritm
        String algorithm = conf.getString(DETECTION_ALGO);
        LOG.debug("[predictor] detection algorithm: " + algorithm);

        if (algorithm.equals("missProbability")) {
            detectionAlgorithm = DetectionAlgorithm.MissProbability;
        } else if (algorithm.equals("missRate")) {
            detectionAlgorithm = DetectionAlgorithm.MissRate;

            //max probability state index
            maxStateProbIndex = new int[markovModel.getNumStates()];
            for (int i = 0; i < markovModel.getNumStates(); ++i) {
                int maxProbIndex = -1;
                double maxProb = -1;
                for (int j = 0; j < markovModel.getNumStates(); ++j) {
                    if (markovModel.getStateTransitionProb()[i][j] > maxProb) {
                        maxProb = markovModel.getStateTransitionProb()[i][j];
                        maxProbIndex = j;
                    }
                }
                maxStateProbIndex[i] = maxProbIndex;
            }
        } else if (algorithm.equals("entropyReduction")) {
            detectionAlgorithm = DetectionAlgorithm.EntropyReduction;

            //entropy per source state
            entropy = new double[markovModel.getNumStates()];
            for (int i = 0; i < markovModel.getNumStates(); ++i) {
                double ent = 0;
                for (int j = 0; j < markovModel.getNumStates(); ++j) {
                    ent  += -markovModel.getStateTransitionProb()[i][j] * Math.log(markovModel.getStateTransitionProb()[i][j]);
                }
                entropy[i] = ent;
            }
        } else {
            //error
            String msg = "The detection algorithm '" + algorithm + "' does not exist";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        //metric threshold
        metricThreshold = conf.getDouble(METRIC_THRESHOLD);
        LOG.debug("[predictor] the threshold is: " + metricThreshold);
    }

    @Override
    public Prediction execute(String entityID, String record) {
        double score = 0;

        List<String> recordSeq = records.get(entityID);
        if (null == recordSeq) {
            recordSeq = new ArrayList<>();
            records.put(entityID, recordSeq);
        }

        //add and maintain size
        recordSeq.add(record);
        if (recordSeq.size() > stateSeqWindowSize) {
            recordSeq.remove(0);
        }

        String[] stateSeq = null;
        if (localPredictor) {
            //local metric
            LOG.debug("local metric,  seq size " + recordSeq.size());

            if (recordSeq.size() == stateSeqWindowSize) {
                stateSeq = new String[stateSeqWindowSize];
                for (int i = 0; i < stateSeqWindowSize; ++i) {
                    stateSeq[i] = recordSeq.get(i).split(",")[stateOrdinal];
                    LOG.debug("[predictor] size={}, stateseq[{}][1]={}", recordSeq.size(), i, stateSeq[i]);
                }
                score = getLocalMetric(stateSeq);
            }
        } else {
            //global metric
            LOG.debug("global metric");

            if (recordSeq.size() >= 2) {
                stateSeq = new String[2];

                for (int i = stateSeqWindowSize - 2, j =0; i < stateSeqWindowSize; ++i) {
                    stateSeq[j++] = recordSeq.get(i).split(",")[stateOrdinal];
                }

                Pair<Double,Double> params = globalParams.get(entityID);

                if (null == params) {
                    params = new Pair<>(0.0, 0.0);
                    globalParams.put(entityID, params);
                }

                score = getGlobalMetric(stateSeq, params);
            }
        }

        //outlier
        LOG.debug("outlier: metric " + entityID + ":" + score);

        Prediction prediction = new Prediction(entityID, score, stateSeq, (score > metricThreshold));

        if (score > metricThreshold) {
            /*
            StringBuilder stBld = new StringBuilder(entityID);
            stBld.append(" : ");
            for (String st : stateSeq) {
                stBld.append(st).append(" ");
            }
            stBld.append(": ");
            stBld.append(score);
            jedis.lpush(outputQueue,  stBld.toString());
            */
            // should return the score and state sequence
            // should say if is an outlier or not
        }

        return prediction;
    }


    /**
     * @param stateSeq
     * @return
     */
    private double getLocalMetric(String[] stateSeq) {
        double metric = 0;
        double[] params = new double[2];
        params[0] = params[1] = 0;

        if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
            missProbability(stateSeq, params);
        } else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
            missRate(stateSeq, params);
        } else {
            entropyReduction( stateSeq, params);
        }

        metric = params[0] / params[1];
        return metric;
    }


    /**
     * @param stateSeq
     * @return
     */
    private double getGlobalMetric(String[] stateSeq, Pair<Double,Double> globParams) {
        double metric = 0;
        double[] params = new double[2];
        params[0] = params[1] = 0;

        if (detectionAlgorithm == DetectionAlgorithm.MissProbability) {
            missProbability(stateSeq, params);
        } else if (detectionAlgorithm == DetectionAlgorithm.MissRate) {
            missRate(stateSeq, params);
        } else {
            entropyReduction( stateSeq, params);
        }

        globParams.setLeft(globParams.getLeft() + params[0]);
        globParams.setRight(globParams.getRight() + params[1]);
        metric = globParams.getLeft() / globParams.getRight();
        return metric;
    }

    /**
     * @param stateSeq
     * @return
     */
    private void missProbability(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);

            LOG.debug("state prob index:" + prState + " " + cuState);

            //add all probability except target state
            for (int j = 0; j < markovModel.getStates().size(); ++ j) {
                if (j != cuState)
                    params[0] += markovModel.getStateTransitionProb()[prState][j];
            }
            params[1] += 1;
        }

        LOG.debug("params:" + params[0] + ":" + params[1]);
    }


    /**
     * @param stateSeq
     * @return
     */
    private void missRate(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);
            params[0] += (cuState == maxStateProbIndex[prState]? 0 : 1);
            params[1] += 1;
        }
    }

    /**
     * @param stateSeq
     * @return
     */
    private void entropyReduction(String[] stateSeq, double[] params) {
        int start = localPredictor? 1 :  stateSeq.length - 1;
        for (int i = start; i < stateSeq.length; ++i ){
            int prState = markovModel.getStates().indexOf(stateSeq[i -1]);
            int cuState = markovModel.getStates().indexOf(stateSeq[i ]);
            params[0] += (cuState == maxStateProbIndex[prState]? 0 : 1);
            params[1] += 1;
        }
    }
}
