/**
 *  @file    constants.hpp
 *  @author  Alessandra Fais
 *  @date    14/06/2019
 *
 *  @brief Definition of useful constants
 */

#ifndef TRAFFICMONITORING_CONSTANTS_HPP
#define TRAFFICMONITORING_CONSTANTS_HPP

#include <string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology name
const string topology_name = "TrafficMonitoring";
const string source_name = "source";
const string map_match_name = "map_matcher";
const string speed_calc_name = "speed_calculator";
const string sink_name = "sink";

typedef enum { BEIJING, DUBLIN } city;

/// information contained in each record in the Beijing dataset
typedef enum { TAXI_ID_FIELD, NID_FIELD, DATE_FIELD, TAXI_LATITUDE_FIELD, TAXI_LONGITUDE_FIELD,
               TAXI_SPEED_FIELD, TAXI_DIRECTION_FIELD } beijing_record_field;

/// information contained in each record in the Dublin dataset
typedef enum { TIMESTAMP_FIELD, LINE_ID_FIELD, BUS_DIRECTION_FIELD, JOURNEY_PATTERN_ID_FIELD, TIME_FRAME_FIELD,
               VEHICLE_JOURNEY_ID_FIELD, OPERATOR_FIELD, CONGESTION_FIELD, BUS_LONGITUDE_FIELD, BUS_LATITUDE_FIELD,
               DELAY_FIELD, BLOCK_ID_FIELD, BUS_ID_FIELD, STOP_ID_FIELD, AT_STOP_ID_FIELD } dublin_record_field;

/// Beijing bounding box
const double beijing_lat_min = 39.689602;
const double beijing_lat_max = 40.122410;
const double beijing_lon_min = 116.105789;
const double beijing_lon_max = 116.670021;

/// Dublin bounding box
const double dublin_lat_min = 53.28006;
const double dublin_lat_max = 53.406071;
const double dublin_lon_min = -6.381911;
const double dublin_lon_max = -6.141994;

/// application parameters
city _monitored_city = BEIJING;     // user can choose between two options: BEIJING and DUBLIN

const string _beijing_input_file = "../../Datasets/TM/taxi-traces.csv";           // path of the Beijing dataset to be used
const string _dublin_input_file = "../data/bus-traces_20130101.csv";    // path of the Dublin dataset to be used

const string _beijing_shapefile = "../../Datasets/TM/beijing/roads.shp";          // path of the Beijing shape file
const string _dublin_shapefile = "../data/dublin/roads.shp";            // path of the Dublin shape file

size_t _road_win_size = 1000;

#endif //TRAFFICMONITORING_CONSTANTS_HPP
