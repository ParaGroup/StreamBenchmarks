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

#ifndef YSB_SINK_HPP
#define YSB_SINK_HPP

#include <algorithm>
#include <iomanip>
#include <ff/ff.hpp>
#include "../util/cli_util.hpp"
#include "../util/result.hpp"
#include "../util/sampler.hpp"
#include "../util/metric_group.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/** 
 *  @class Sink_Functor
 *  
 *  @brief Defines the logic of the Sink
 */ 
class Sink_Functor {
private:
    long sampling;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t processed;                       // tuples counter
    // runtime information
    size_t parallelism;
    size_t replica_id;
    util::Sampler latency_sampler;
public:
    /**
     *  @brief Constructor
     *
     *  @param _sampling sampling rate
     *  @param _app_start_time application starting time
     */
    Sink_Functor(const long _sampling,
                 const unsigned long _app_start_time):
                 sampling(_sampling),
                 app_start_time(_app_start_time),
                 current_time(_app_start_time),
                 processed(0),
                 latency_sampler(_sampling) {}

    /**
     * @brief Print results and evaluate latency statistics
     *
     * @param r input tuple
     */
    void operator()(optional<result_t>& r, RuntimeContext& rc) {
        if (r) {
            if (processed == 0) {
                parallelism = rc.getParallelism();
                replica_id = rc.getReplicaIndex();
            }
            //cout << "Sink riceve risultato chiave " << (*r).cmp_id << endl;

            // always evaluate latency when compiling with FF_BOUNDED_BUFFER MACRO set
            unsigned long tuple_latency = (current_time_nsecs() - (*r).lastUpdate) / 1e03;
            processed++;        // tuples counter
            current_time = current_time_usecs();
            latency_sampler.add(tuple_latency, current_time);

        }
        else {     // EOS
            /*cout << "[Sink] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1000000L
                 << " s, processed: " << processed
                 << endl;*/
            util::metric_group.add("latency", latency_sampler);
        }
    }
};

#endif //YSB_SINK_HPP
