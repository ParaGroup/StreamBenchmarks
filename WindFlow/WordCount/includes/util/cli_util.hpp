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

#ifndef WORDCOUNT_CLI_UTIL_HPP
#define WORDCOUNT_CLI_UTIL_HPP

#include<iomanip>
#include<iostream>
#include<string>
#include<vector>
#include<getopt.h>
#include "constants.hpp"
#include "result.hpp"

using namespace std;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"rate", REQUIRED, 0, 'r'},      // pipe start (source) parallelism degree
        {"sampling", REQUIRED, 0, 's'},   // predictor parallelism degree
        {"batch", REQUIRED, 0, 'b'},
        {"parallelism", REQUIRED, 0, 'p'},        // pipe end (sink) parallelism degree
        {"chaining", NONE, 0, 'c'},
        {0, 0, 0, 0}
};

// how to run the application
const string intro = "Run WordCount choosing one of the following ways:";
const string run_mode1 = " --nsource [source_par_deg] --nsplitter [splitter_par_deg] --ncounter [counter_par_deg] --nsink [sink_par_deg] --rate [stream_gen_rate]";
const string run_mode2 = " --pardeg [par_deg_for_all_nodes] --rate [stream_gen_rate]";
const string run_help = " --help";

// information about application
const string app_descr = "Executing WordCount with parameters:";
const string file_str = "* file path: ";
const string source_str = "* source parallelism degree: ";
const string splitter_str = "* splitter parallelism degree: ";
const string counter_str = "* counter parallelism degree: ";
const string sink_str = "* sink parallelism degree: ";
const string rate_str = "* rate: ";
const string app_error = "Error executing WordCount topology";
const string app_termination = "Terminated execution of WordCount topology with cardinality ";

#endif //WORDCOUNT_CLI_UTIL_HPP
