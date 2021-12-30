/**
 * @file    cli_util.hpp
 * @author  Alessandra Fais
 * @date    18/07/2019
 *
 * @brief Util for parsing command line options and printing information on stdout
 *
 * This file contains functions and constants used for parsing command line options
 * and for showing information about the application on stdout.
 */

#ifndef SPIKEDETECTION_CLI_UTIL_HPP
#define SPIKEDETECTION_CLI_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <getopt.h>
#include "constants.hpp"
#include "tuple.hpp"

using namespace std;
using record_t = tuple<string, string, int, int, double, double, double, double>;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"rate", REQUIRED, 0, 'r'},      // pipe start (source) parallelism degree
        {"sampling", REQUIRED, 0, 's'},   // predictor parallelism degree
        {"parallelism", REQUIRED, 0, 'p'},        // pipe end (sink) parallelism degree
        {"batch", REQUIRED, 0, 'b'},        // batch size
        {"keys", REQUIRED, 0, 'k'},        // number of keys
        {"chaining", NONE, 0, 'c'},
        {0, 0, 0, 0}
};

// how to run the application
const string intro = "Run SpikeDetection choosing one of the following ways:";
const string run_mode1 = " --nsource [source_par_deg] --naverage [avg_calc_par_deg] --ndetector [detector_par_deg] --nsink [sink_par_deg] --rate [stream_gen_rate]";
const string run_mode2 = " --pardeg [par_deg_for_all_nodes] --rate [stream_gen_rate]";
const string run_help = " --help";

// information about application
const string app_descr = "Executing SpikeDetection with parameters:";
const string file_str = "* file path: ";
const string source_str = "* source parallelism degree: ";
const string avg_calc_str = "* average calculator parallelism degree: ";
const string detector_str = "* detector parallelism degree: ";
const string sink_str = "* sink parallelism degree: ";
const string rate_str = "* rate: ";

const string app_error = "Error executing SpikeDetection topology";
const string app_termination = "Terminated execution of SpikeDetection topology with cardinality ";

inline void print_help(char* arg) {
    cout << intro << endl
         << arg
         << run_mode1 << endl
         << arg
         << run_mode2 << endl
         << arg
         << run_help << endl;
}

inline void print_app_descr(string f, size_t source, size_t avg_calc, size_t detector, size_t sink, int rate) {
    cout << app_descr << endl
         << file_str << f << endl
         << source_str << source << endl
         << avg_calc_str << avg_calc << endl
         << detector_str << detector << endl
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
inline void print_parsing_info(const vector<record_t>& parsed_file, size_t all_records, size_t incomplete_records) {
    cout << "[main] parsed " << all_records << " records ("
         << incomplete_records << " incomplete, "
         << all_records - incomplete_records << " valid)" << endl;
    for (auto r : parsed_file)
        cout << get<DEVICE_ID_FIELD>(r) << " - "
             << get<TEMP_FIELD>(r) << " - "
             << get<HUMID_FIELD>(r) << " - "
             << get<LIGHT_FIELD>(r) << " - "
             << get<VOLT_FIELD>(r) << endl;
}

// information about dataset (testing)
inline void print_dataset(const vector<tuple_t>& dataset) {
    cout << "[main] dataset size: " << dataset.size() << endl;
    for (auto t : dataset)
        cout << t.property_value << " - "
             << t.incremental_average << " - "
             << t.key << " - " << endl;
}

// information about windows (testing)
inline void print_window(const deque<double>& win) {
    cout << "[AverageCalculator] values in window [";
    for (double t : win)
        cout << t << " ";
    cout << "]" << endl;
}

// information about tuple content (testing)
inline void print_tuple(const string& msg, const tuple_t& t) {
    cout << msg
         << t.property_value << " - "
         << t.incremental_average << ", "
         << t.key << " - " << endl;
}

#endif //SPIKEDETECTION_CLI_UTIL_HPP
