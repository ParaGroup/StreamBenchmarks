/** 
 *  @file    cli_util.hpp
 *  @author  Alessandra Fais
 *  @date    17/07/2019
 *  
 *  @brief Util for parsing command line options and printing information on stdout
 *  
 *  This file contains functions and constants used for parsing command line options
 *  and for showing information about the application state on stdout.
 */ 

#ifndef FRAUDDETECTION_CLI_UTIL_HPP
#define FRAUDDETECTION_CLI_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <getopt.h>
#include "tuple.hpp"

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

#endif //FRAUDDETECTION_CLI_UTIL_HPP
