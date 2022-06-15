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

#ifndef SPIKEDETECTION_CLI_UTIL_HPP
#define SPIKEDETECTION_CLI_UTIL_HPP

#include<iomanip>
#include<iostream>
#include<string>
#include<vector>
#include<getopt.h>
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

#endif //SPIKEDETECTION_CLI_UTIL_HPP
