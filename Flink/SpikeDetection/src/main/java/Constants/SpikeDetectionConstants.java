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
 *  Constants peculiar of the SpikeDetection application.
 */ 
public interface SpikeDetectionConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/spikedetection/sd.properties";
    String DEFAULT_TOPO_NAME = "SpikeDetection";
    double DEFAULT_THRESHOLD = 0.03d;

    interface Conf {
        String RUNTIME = "sd.runtime_sec";
        String SPOUT_PATH = "sd.spout.path";
        String PARSER_VALUE_FIELD = "sd.parser.value_field";
        String MOVING_AVERAGE_WINDOW = "sd.moving_average.window";
        String SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold";
    }
    
    interface Component extends BaseComponent {
        String MOVING_AVERAGE = "moving_average";
        String SPIKE_DETECTOR = "spike_detector";
    }

    interface Field extends BaseField {
        String DEVICE_ID = "deviceID";
        String VALUE = "value";
        String MOVING_AVG = "movingAverage";
    }

    interface DatasetParsing {
        int DATE_FIELD = 0;
        int TIME_FIELD = 1;
        int EPOCH_FIELD = 2;
        int DEVICEID_FIELD = 3;
        int TEMP_FIELD = 4;
        int HUMID_FIELD = 5;
        int LIGHT_FIELD = 6;
        int VOLT_FIELD = 7;
    }
}
