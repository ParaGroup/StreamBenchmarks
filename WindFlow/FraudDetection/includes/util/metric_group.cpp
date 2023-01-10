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

#include "metric_group.hpp"
#include <algorithm>
#include <fstream>
#include <numeric>
#include <rapidjson/prettywriter.h>
#include <sstream>

using namespace rapidjson;

namespace util {

MetricGroup metric_group;

void MetricGroup::add(std::string name, Sampler sampler)
{
    // XXX this is not time critical, using a simple mutex lock is good enough
    std::lock_guard lock(mutex_);
    auto &samplers = map_[name];
    samplers.push_back(sampler);
}

void MetricGroup::dump_all()
{
    for (auto &it : map_) {
        Metric metric = get_metric(it.first);
        metric.dump();
    }
}

Metric MetricGroup::get_metric(std::string name)
{
    Metric metric(name);

    // consume all the groups
    auto &samplers = map_.at(name);
    while (!samplers.empty()) {
        auto sampler = samplers.back();
        metric.total(sampler.total());

        // add all the values from the sampler
        for (double value : sampler.values()) {
            metric.add(value);
        }

        // discard it
        samplers.pop_back();
    }

    return metric;
}

}
