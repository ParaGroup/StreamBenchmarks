/** 
 *  @file    constants.hpp
 *  @author  Alessandra Fais
 *  @date    16/05/2019
 *
 *  @brief Definition of useful constants
 */

#ifndef SPIKEDETECTION_CONSTANTS_HPP
#define SPIKEDETECTION_CONSTANTS_HPP

#include <string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology name
const string topology_name = "SpikeDetection";
const string source_name = "source";
const string avg_calc_name = "average_calculator";
const string detector_name = "detector";
const string sink_name = "sink";

/// information contained in each record in the dataset
typedef enum { DATE_FIELD, TIME_FIELD, EPOCH_FIELD, DEVICE_ID_FIELD, TEMP_FIELD, HUMID_FIELD, LIGHT_FIELD, VOLT_FIELD } record_field;

/// fields that can be monitored by the user
typedef enum { TEMPERATURE, HUMIDITY, LIGHT, VOLTAGE } monitored_field;

/// model parameters
size_t _moving_avg_win_size = 1000;
monitored_field _field = TEMPERATURE;
double _threshold = 0.25; // <-- original value is 0.025

const string _input_file = "../../Datasets/SD/sensors.dat";

#endif //SPIKEDETECTION_CONSTANTS_HPP
