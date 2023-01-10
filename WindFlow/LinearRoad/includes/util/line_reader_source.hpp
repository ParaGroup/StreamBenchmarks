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

#include<util/sampler.hpp>
#include<util/simple_tuple.hpp>
#include<ostream>
#include<string>
#include<vector>
#include<windflow.hpp>

namespace util {

class LineReaderSource
{
public:

    struct Tuple
    {
        const std::string *line;
        uint64_t ts;
    };

public:

    LineReaderSource(int run_time_sec, int gen_rate, const std::string &path);

    void operator ()(wf::Source_Shipper<Tuple> &shipper, wf::RuntimeContext &rc);

private:

    void read_all(const std::string &path);

private:

    const int run_time_sec_;
    const int gen_rate_;
    util::Sampler throughput_;
    std::vector<std::string> data_;
    std::size_t index_;
    std::size_t counter_;
    long epoch_;

    void active_delay(unsigned long waste_time);
};

    std::ostream &operator <<(std::ostream &os, const LineReaderSource::Tuple &tuple);
}
