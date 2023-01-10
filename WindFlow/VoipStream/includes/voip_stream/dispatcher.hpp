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

#include<voip_stream/bloom_filter.hpp>
#include<voip_stream/call_detail_record.hpp>
#include<voip_stream/parser.hpp>
#include<string>

namespace voip_stream {

class Dispatcher
{
public:

    struct Tuple
    {
        typedef std::string Key;

        uint64_t ts;
        bool new_callee;
        CallDetailRecord cdr;
        double ecr; // XXX used by the "reordering" variant
    };

public:

    Dispatcher();

    Dispatcher::Tuple operator ()(const Parser::Tuple &tuple, wf::RuntimeContext &rc);

private:

    void rotate_filters();

private:

    BloomFilter<std::string> detector_;
    BloomFilter<std::string> learner_;
    double cycle_threshold_;
};

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple);

}
