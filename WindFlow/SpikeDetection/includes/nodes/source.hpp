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

#ifndef SPIKEDETECTION_LIGHT_SOURCE_HPP
#define SPIKEDETECTION_LIGHT_SOURCE_HPP

#include<fstream>
#include<vector>
#include<ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sent_tuples;

// Source_Functor class
class Source_Functor
{
private:
    const vector<tuple_t> &dataset;
    int rate;
    size_t next_tuple_idx;
    long generated_tuples;
    unsigned long app_start_time;
    unsigned long current_time;
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
    Source_Functor(const vector<tuple_t> &_dataset,
                   const int _rate,
                   const unsigned long _app_start_time,
                   const size_t _batch_size):
                   dataset(_dataset),
                   rate(_rate),
                   next_tuple_idx(0),
                   generated_tuples(0),
                   app_start_time(_app_start_time),
                   current_time(_app_start_time),
                   batch_size(_batch_size) {}

    // operator() method
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        current_time = current_time_nsecs(); // get the current time
        while (current_time - app_start_time <= app_run_time) // generation loop
        {
            tuple_t t(dataset.at(next_tuple_idx));
            if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            }
            // t.ts = current_time;
            shipper.pushWithTimestamp(std::move(t), current_time); // send the next tuple
            generated_tuples++;
            next_tuple_idx = (next_tuple_idx + 1) % dataset.size();
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
    }

    // Destructor
    ~Source_Functor() {}
};

#endif //SPIKEDETECTION_LIGHT_SOURCE_HPP
