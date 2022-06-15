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

#include "metric.hpp"
#include <algorithm>
#include <fstream>
#include <numeric>
#include <rapidjson/prettywriter.h>
#include <sstream>

using namespace rapidjson;

namespace util {

Metric::Metric(const std::string &name)
    : name_(name)
{}

void Metric::add(double value)
{
    samples_.push_back(value);
}

void Metric::total(long total)
{
    total_ = total;
}

void Metric::dump()
{
    StringBuffer buffer;
    PrettyWriter<rapidjson::StringBuffer> writer(buffer);

    writer.StartObject();

    writer.Key("name");
    writer.String(name_.c_str());

    writer.Key("samples");
    writer.Uint(samples_.size());

    writer.Key("total");
    writer.Uint(total_);

    writer.Key("mean");
    writer.Double(std::accumulate(samples_.begin(), samples_.end(), 0.0) / samples_.size());

    auto minmax = std::minmax_element(samples_.begin(), samples_.end());
    double min = *minmax.first;
    double max = *minmax.second;

    writer.Key("0");
    writer.Double(min);

    // XXX no interpolation since we are dealing with *many* samples

    // add percentiles
    for (auto percentile : {0.05, 0.25, 0.5, 0.75, 0.95}) {
        auto pointer = samples_.begin() + samples_.size() * percentile;
        std::nth_element(samples_.begin(), pointer, samples_.end());
        auto label = std::to_string(int(percentile * 100));
        auto value = *pointer;
        writer.Key(label.c_str());
        writer.Double(value);
    }

    writer.Key("100");
    writer.Double(max);

    writer.EndObject();

    // format the file name
    std::ostringstream file_name;
    file_name << "metric_" << name_ << ".json";

    // serialize the object to file
    std::ofstream fs(file_name.str());
    fs << buffer.GetString();
}

}
