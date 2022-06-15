/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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

#ifndef YSB_CLI_UTIL_HPP
#define YSB_CLI_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <getopt.h>
#include "constants.hpp"
#include "event.hpp"
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
const string intro = "Run YSB choosing one of the following ways:";
const string run_mode1 = " --nsource [source_par_deg] --nfilter [filter_par_deg] --njoiner [joiner_par_deg] --nwinAgg [winAgg_par_deg] --nsink [sink_par_deg] --rate [stream_gen_rate]";
const string run_mode2 = " --pardeg [par_deg_for_all_nodes] --rate [stream_gen_rate]";
const string run_help = " --help";

// information about application
const string app_descr = "Executing YSB with parameters:";
const string file_str = "* file path: ";
const string source_str = "* source parallelism degree: ";
const string filter_str = "* filter parallelism degree: ";
const string joiner_str = "* joiner parallelism degree: ";
const string winAgg_str = "* winAggregate parallelism degree: ";
const string sink_str = "* sink parallelism degree: ";
const string rate_str = "* rate: ";

const string app_error = "Error executing YSB topology";
const string app_termination = "Terminated execution of YSB topology with cardinality ";

inline void print_help(char* arg) {
    cout << intro << endl
         << arg
         << run_mode1 << endl
         << arg
         << run_mode2 << endl
         << arg
         << run_help << endl;
}

inline void print_app_descr(size_t source, size_t filter, size_t joiner, size_t winAgg, size_t sink, int rate) {
    cout << app_descr << endl
         << source_str << source << endl
         << filter_str << filter << endl
         << joiner_str << joiner << endl
         << winAgg_str << winAgg << endl
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

#endif //YSB_CLI_UTIL_HPP
