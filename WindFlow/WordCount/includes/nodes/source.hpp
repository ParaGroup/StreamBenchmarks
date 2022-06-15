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

#ifndef WORDCOUNT_SOURCE_HPP
#define WORDCOUNT_SOURCE_HPP

#include<fstream>
#include<vector>
#include<ff/ff.hpp>
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> total_lines;
extern atomic<long> total_bytes;

// Source_Functor class
class Source_Functor
{
private:
    const vector<string> &dataset;
    int rate;
    size_t next_tuple_idx;
    long generated_tuples;
    long generated_bytes;
    unsigned long app_start_time;
    unsigned long current_time;
    unsigned long interval;
    size_t batch_size;

    // active_delay method
    void active_delay(unsigned long waste_time)
    {
        auto start_time = current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    // Constructor
    Source_Functor(const vector<string> &_dataset,
                   const int _rate,
                   const unsigned long _app_start_time,
                   const size_t _batch_size):
                   dataset(_dataset),
                   rate(_rate),
                   next_tuple_idx(0),
                   generated_tuples(0),
                   generated_bytes(0),
                   app_start_time(_app_start_time),
                   current_time(_app_start_time),
                   interval(1000000L),
                   batch_size(_batch_size) {}

    // operator() method
    void operator()(Source_Shipper<string> &shipper)
    {
        current_time = current_time_nsecs(); // get the current time
        while (current_time - app_start_time <= app_run_time) // generation loop
        {
            if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            }
            generated_tuples++;
            generated_bytes += (dataset.at(next_tuple_idx)).size();
            shipper.pushWithTimestamp(dataset.at(next_tuple_idx), current_time); // send the next tuple
            next_tuple_idx = (next_tuple_idx + 1) % dataset.size(); // index of the next tuple to be sent (if any)
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
        }
        total_lines.fetch_add(generated_tuples);
        total_bytes.fetch_add(generated_bytes);
    }
};

#endif //WORDCOUNT_SOURCE_HPP
