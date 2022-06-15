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

#ifndef TRAFFICMONITORING_CLI_UTIL_HPP
#define TRAFFICMONITORING_CLI_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <getopt.h>
#include "constants.hpp"
#include "tuple.hpp"
#include "result.hpp"

using namespace std;
using beijing_record_t = tuple<int, int, string, double, double, double, int>;
using dublin_record_t = tuple<long, int, int, string, string, int, string, int, double, double, int, int, int, int, int>;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"rate", REQUIRED, 0, 'r'},      // pipe start (source) parallelism degree
        {"sampling", REQUIRED, 0, 's'},   // predictor parallelism degree
        {"batch", REQUIRED, 0, 'b'},        // pipe end (sink) parallelism degree
        {"parallelism", REQUIRED, 0, 'p'},        // pipe end (sink) parallelism degree
        {"chaining", NONE, 0, 'c'},
        {0, 0, 0, 0}
};

// how to run the application
const string intro = "Run TrafficMonitoring choosing one of the following ways:";
const string run_mode1 = " --nsource [source_par_deg] --nmatcher [map_match_par_deg] --ncalculator [calculator_par_deg] --nsink [sink_par_deg] --rate [stream_gen_rate]";
const string run_mode2 = " --pardeg [par_deg_for_all_nodes] --rate [stream_gen_rate]";
const string run_help = " --help";

// information about application
const string app_descr = "Executing TrafficMonitoring with parameters:";
const string file_str = "* file path: ";
const string source_str = "* source parallelism degree: ";
const string map_match_str = "* map matcher parallelism degree: ";
const string calculator_str = "* speed calculator parallelism degree: ";
const string sink_str = "* sink parallelism degree: ";
const string rate_str = "* rate: ";

const string app_error = "Error executing TrafficMonitoring topology";
const string app_termination = "Terminated execution of TrafficMonitoring topology with cardinality ";

inline void print_help(char* arg) {
    cout << intro << endl
         << arg
         << run_mode1 << endl
         << arg
         << run_mode2 << endl
         << arg
         << run_help << endl;
}

inline void print_app_descr(string f, size_t source, size_t map_match, size_t calculator, size_t sink, int rate) {
    cout << app_descr << endl
         << file_str << f << endl
         << source_str << source << endl
         << map_match_str << map_match << endl
         << calculator_str << calculator << endl
         << sink_str << sink << endl
         << rate_str << rate << endl;
}

inline void print_summary(const atomic<long>& sent_tuples, double elapsed_time_seconds, double tot_average_latency) {
    cout << "[SUMMARY] generated " << sent_tuples << " (tuples)" << endl;
    cout << "[SUMMARY] elapsed time " << elapsed_time_seconds << " (seconds)" << endl;
    cout << "[SUMMARY] bandwidth "
         << sent_tuples / elapsed_time_seconds << " (tuples/s)" << endl;
    cout << "[SUMMARY] average latency "
         << fixed << setprecision(5) << tot_average_latency << " (ms) " <<  endl;
}

// information about parsed data (testing)
inline void print_taxi_parsing_info(const vector<beijing_record_t>& parsed_file, size_t all_records, size_t incomplete_records) {
    cout << "[main] parsed " << all_records << " beijing records ("
         << incomplete_records << " incomplete, "
         << all_records - incomplete_records << " valid)" << endl;
    for (auto r : parsed_file)
        cout << get<TAXI_ID_FIELD>(r) << " - "
             << get<NID_FIELD>(r) << " - "
             << get<DATE_FIELD>(r) << " - "
             << get<TAXI_LATITUDE_FIELD>(r) << " - "
             << get<TAXI_LONGITUDE_FIELD>(r) << " - "
             << get<TAXI_SPEED_FIELD>(r) << " - "
             << get<TAXI_DIRECTION_FIELD>(r) << endl;
}

inline void print_bus_parsing_info(const vector<dublin_record_t>& parsed_file, size_t all_records, size_t incomplete_records) {
    cout << "[main] parsed " << all_records << " dublin records ("
         << incomplete_records << " incomplete, "
         << all_records - incomplete_records << " valid)" << endl;
    for (auto r : parsed_file)
        cout << get<TIMESTAMP_FIELD>(r) << " - "
             << get<LINE_ID_FIELD>(r) << " - "
             << get<BUS_DIRECTION_FIELD>(r) << " - "
             << get<JOURNEY_PATTERN_ID_FIELD>(r) << " - "
             << get<TIME_FRAME_FIELD>(r) << " - "
             << get<VEHICLE_JOURNEY_ID_FIELD>(r) << " - "
             << get<OPERATOR_FIELD>(r) << " - "
             << get<CONGESTION_FIELD>(r) << " - "
             << get<BUS_LONGITUDE_FIELD>(r) << " - "
             << get<BUS_LATITUDE_FIELD>(r) << " - "
             << get<DELAY_FIELD>(r) << " - "
             << get<BLOCK_ID_FIELD>(r) << " - "
             << get<BUS_ID_FIELD>(r) << " - "
             << get<STOP_ID_FIELD>(r) << " - "
             << get<AT_STOP_ID_FIELD>(r) << endl;
}

// information about dataset (testing)
inline void print_dataset(const vector<tuple_t>& dataset) {
    cout << "[main] dataset size: " << dataset.size() << endl;
    for (auto t : dataset)
        cout << t.latitude << " - "
             << t.longitude << " - "
             << t.speed << " - "
             << t.direction << " - "
             << t.key << endl;
}

// information about input tuple content (testing)
inline void print_tuple(const string& msg, const tuple_t& t) {
    cout << msg
         << t.latitude << " - "
         << t.longitude << " - "
         << t.speed << " - "
         << t.direction << ", "
         << t.key << endl;
}

// information about tuple result content (testing)
inline void print_result(const string& msg, const result_t& r) {
    cout << msg
         << r.speed << ", "
         << r.key << endl;
}

#endif //TRAFFICMONITORING_CLI_UTIL_HPP
