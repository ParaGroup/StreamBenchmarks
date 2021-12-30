/**
 * @file    cli_util.hpp
 * @author  Alessandra Fais
 * @date    17/07/2019
 *
 * @brief Util for parsing command line options and printing information on stdout
 *
 * This file contains functions and constants used for parsing command line options
 * and for showing information about the application state on stdout.
 */

#ifndef FRAUDDETECTION_CLI_UTIL_HPP
#define FRAUDDETECTION_CLI_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <getopt.h>
#include "tuple.hpp"
#include "result.hpp"

using namespace std;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"rate", REQUIRED, 0, 'r'},      // pipe start (source) parallelism degree
        {"sampling", REQUIRED, 0, 's'},   // predictor parallelism degree
        {"parallelism", REQUIRED, 0, 'p'},        // pipe end (sink) parallelism degree
        {"batch", REQUIRED, 0, 'b'},        // source and predictor batch size
        {"keys", REQUIRED, 0, 'k'},        // number of keys
        {"chaining", NONE, 0, 'c'},
        {0, 0, 0, 0}
};

// how to run the application
const string intro = "Run FraudDetection choosing one of the following ways:";
const string run_mode1 = " --nsource [source_par_deg] --npredictor [predictor_par_deg] --nsink [sink_par_deg] --rate [stream_gen_rate]";
const string run_mode2 = " --pardeg [par_deg_for_all_nodes] --rate [stream_gen_rate]";
const string run_help = " --help";

// information about application
const string app_descr = "Executing FraudDetection with parameters:";
const string file_str = "* file path: ";
const string source_str = "* source parallelism degree: ";
const string predictor_str = "* predictor parallelism degree: ";
const string sink_str = "* sink parallelism degree: ";
const string rate_str = "* rate: ";

const string app_error = "Error executing FraudDetection topology";
const string app_termination = "Terminated execution of FraudDetection topology with cardinality ";

inline void print_summary(const atomic<long>& sent_tuples, double elapsed_time_seconds, double tot_average_latency) {
    cout << "[SUMMARY] generated " << sent_tuples << " (tuples)" << endl;
    cout << "[SUMMARY] elapsed time " << elapsed_time_seconds << " (seconds)" << endl;
    cout << "[SUMMARY] bandwidth "
         << sent_tuples / elapsed_time_seconds << " (tuples/s)" << endl;
    cout << "[SUMMARY] average latency "
         << fixed << setprecision(5) << tot_average_latency << " (ms) " <<  endl;
}

// information about the model (testing)
inline void print_model_parameters(const string& _model_file, size_t records_win_size, size_t state_position, int alg, double threshold) {
    cout << "Model parameters are: "
         << " file " << _model_file << endl
         << " records_win_size " << records_win_size << endl
         << " state_position (in the record string) " << state_position << endl
         << " algorithm " << alg << endl
         << " threshold " << threshold << endl;
}

inline void print_window(const vector<string>& states_sequence) {
    cout << "Compute local metric for states in window [ ";
    for (auto s : states_sequence) cout << s << " ";
    cout << "]" << endl;
}

inline void print_prob_indexes(const string& prev_state, const string& cur_state, size_t prev_state_idx, size_t cur_state_idx) {
    cout << "State probability indexes are: "
         << "[prev state] " << prev_state << " at " << prev_state_idx
         << ", [cur state] " << cur_state << " at " << cur_state_idx << endl;
}

inline void print_fraudolent_sequence(const vector<string>& states_sequence, double score, double threshold) {
    cout << "Fraudolent sequence: ";
    for (string s : states_sequence) {
        cout << s << " ";
    }
    cout << endl << "Score: " << score << " Threshold: " << threshold << endl;
}

// information about tuple and result content (testing)
inline void print_tuple(const string& msg, const tuple_t& t) {
    cout << msg
         << t.entity_id << " - "
         << t.record << ", "
         << t.key << endl;
}

inline void print_result(const string& msg, const result_t& t) {
    cout << msg
         << t.entity_id << " - "
         << t.score << " - [ "
         //<< t.states << "], "
         << t.key << " - " << endl;
}

#endif //FRAUDDETECTION_CLI_UTIL_HPP
