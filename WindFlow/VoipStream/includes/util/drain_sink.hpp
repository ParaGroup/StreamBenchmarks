/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
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

#pragma once

#include<util/log.hpp>
#include<util/metric_group.hpp>
#include<windflow.hpp>

namespace util {

template <typename Tuple>
class DrainSink
{
public:

    DrainSink(long sampling_rate)
        : latency_(sampling_rate)
    {}

    void operator ()(std::optional<Tuple> &tuple, wf::RuntimeContext &rc)
    {
        if (tuple) {
            DEBUG_LOG("sink::tuple " << *tuple);

            // compute the latency for this tuple
            auto timestamp = wf::current_time_nsecs();
            auto latency = (timestamp - tuple->ts) / 1e3; // microseconds
            latency_.add(latency, timestamp);
        } else {
            DEBUG_LOG("sink::finished");

            // end of stream, dump metrics
            util::metric_group.add("latency", latency_);
        }
    }

private:

    util::Sampler latency_;
};

}
