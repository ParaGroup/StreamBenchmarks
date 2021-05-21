/**
 *  @file    constants.hpp
 *  @author  Alessandra Fais
 *  @date    11/05/2019
 *
 *  @brief Definition of useful constants
 */

#ifndef FRAUDDETECTION_CONSTANTS_HPP
#define FRAUDDETECTION_CONSTANTS_HPP

#include <string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology names
const string topology_name = "FraudDetection";
const string heavy_source_name = "heavy_source";
const string light_source_name = "light_source";
const string predictor_name = "predictor";
const string sink_name = "sink";

/// Markov model parameters
typedef enum { MISS_PROBABILITY, MISS_RATE, ENTROPY_REDUCTION } detection_algorithm;

size_t _records_win_size = 5;
size_t _state_position = 1;
detection_algorithm _alg = MISS_PROBABILITY;
double _threshold = 0.96;

const string _model_file = "../../Datasets/FD/model.txt";
const string _input_file = "../../Datasets/FD/credit-card.dat";

#endif //FRAUDDETECTION_CONSTANTS_HPP
