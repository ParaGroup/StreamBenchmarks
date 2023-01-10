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

#ifndef FRAUDDETECTION_CONSTANTS_HPP
#define FRAUDDETECTION_CONSTANTS_HPP

#include<string>

using namespace std;

// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

// components and topology names
const string topology_name = "FraudDetection";
const string heavy_source_name = "heavy_source";
const string light_source_name = "light_source";
const string predictor_name = "predictor";
const string sink_name = "sink";

// Markov model parameters
typedef enum { MISS_PROBABILITY, MISS_RATE, ENTROPY_REDUCTION } detection_algorithm;

size_t _records_win_size = 5;
size_t _state_position = 1;
detection_algorithm _alg = MISS_PROBABILITY;
double _threshold = 0.96;
const string _model_file = "../../Datasets/FD/model.txt";
const string _input_file = "../../Datasets/FD/credit-card.dat";

#endif //FRAUDDETECTION_CONSTANTS_HPP
