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

#include<voip_stream/constants.hpp>
#include<voip_stream/ct24.hpp>
#include<util/log.hpp>

namespace voip_stream {

CT24::CT24()
    : filter_(CT24_NUM_ELEMENTS, CT24_BUCKETS_PER_ELEMENT, CT24_BETA, CT24_BUCKETS_PER_WORD)
{}

void CT24::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ct24::tuple " << tuple);

    if (tuple.cdr.call_established && tuple.new_callee) {
        filter_.add(tuple.cdr.calling_number, tuple.cdr.call_duration, tuple.cdr.answer_timestamp);
        double call_time = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        FilterTuple result;
        result.ts = tuple.ts;
        result.source = SOURCE_CT24;
        result.rate = call_time;
        result.cdr = tuple.cdr;
        shipper.push(std::move(result));
    }
}

}
