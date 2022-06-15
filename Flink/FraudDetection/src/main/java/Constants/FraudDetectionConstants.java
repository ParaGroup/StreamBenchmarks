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

package Constants;

/** 
 *  @author  Alessandra Fais
 *  @version May 2019
 *  
 *  Constants peculiar of the FraudDetection application.
 */ 
public interface FraudDetectionConstants extends BaseConstants {
    String DEFAULT_MODEL = "frauddetection/model.txt";
    String DEFAULT_PROPERTIES = "/frauddetection/fd.properties";
    String DEFAULT_TOPO_NAME = "FraudDetection";

    interface Conf {
        String RUNTIME            = "fd.runtime_sec";
        String SPOUT_PATH         = "fd.spout.path";
        String PREDICTOR_MODEL    = "fd.predictor.model";
        String MARKOV_MODEL_KEY   = "fd.markov.model.key";
        String LOCAL_PREDICTOR    = "fd.local.predictor";
        String STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size";
        String STATE_ORDINAL      = "fd.state.ordinal";
        String DETECTION_ALGO     = "fd.detection.algorithm";
        String METRIC_THRESHOLD   = "fd.metric.threshold";
    }

    interface Component extends BaseComponent {
        String PREDICTOR = "fraud_predictor";
    }

    interface Field extends BaseField {
        String ENTITY_ID = "entityID";
        String RECORD_DATA = "recordData";
        String SCORE = "score";
        String STATES = "states";
    }
}
