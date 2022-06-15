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

#ifndef SPIKEDETECTION_CONSTANTS_HPP
#define SPIKEDETECTION_CONSTANTS_HPP

#include<string>

using namespace std;

// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

// components and topology name
const string topology_name = "SpikeDetection";
const string source_name = "source";
const string avg_calc_name = "average_calculator";
const string detector_name = "detector";
const string sink_name = "sink";

// information contained in each record in the dataset
typedef enum { DATE_FIELD, TIME_FIELD, EPOCH_FIELD, DEVICE_ID_FIELD, TEMP_FIELD, HUMID_FIELD, LIGHT_FIELD, VOLT_FIELD } record_field;

// fields that can be monitored by the user
typedef enum { TEMPERATURE, HUMIDITY, LIGHT, VOLTAGE } monitored_field;

// model parameters
size_t _moving_avg_win_size = 1000;
monitored_field _field = TEMPERATURE;
double _threshold = 0.25; // <-- original value was 0.025
const string _input_file = "../../Datasets/SD/sensors.dat";

#endif //SPIKEDETECTION_CONSTANTS_HPP
