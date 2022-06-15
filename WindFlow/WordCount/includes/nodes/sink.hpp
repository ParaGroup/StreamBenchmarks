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

#ifndef WORDCOUNT_SINK_HPP
#define WORDCOUNT_SINK_HPP

#include<algorithm>
#include<iomanip>
#include<ff/ff.hpp>
#include "../util/cli_util.hpp"
#include "../util/result.hpp"
#include "../util/sampler.hpp"
#include "../util/metric_group.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Sink_Functor class
class Sink_Functor
{
private:
    long sampling;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t words;
    size_t parallelism;
    size_t replica_id;
    util::Sampler latency_sampler;

public:
    // Constructor
    Sink_Functor(const long _sampling,
                 const unsigned long _app_start_time):
                 sampling(_sampling),
                 app_start_time(_app_start_time),
                 current_time(_app_start_time),
                 words(0),
                 latency_sampler(_sampling) {}

    // operator() method
    void operator()(optional<result_t> &r, RuntimeContext &rc)
    {
        if (r) {
            if (words == 0) {
                parallelism = rc.getParallelism();
                replica_id = rc.getReplicaIndex();
            }
            current_time = current_time_nsecs();
            unsigned long tuple_latency = (current_time - rc.getCurrentTimestamp()) / 1e03;
            words++;// tuples counter
            latency_sampler.add(tuple_latency, current_time);
#if 0
            if (words < 4653) {
                cout << "Ricevuta parola: " << (*r).word << ", count " << (*r).count << endl;
            }
#endif
        }
        else { // EOS
            if (words != 0) {
                /*cout << "[Sink] words: "
                         << words << " (words) "
                         << (bytes_sum / 1048576) << " (MB), "
                         << "bandwidth: "
                         << words / t_elapsed << " (words/s) " << endl;*/
                util::metric_group.add("latency", latency_sampler);
            }
        }
    }
};

#endif //WORDCOUNT_SINK_HPP
