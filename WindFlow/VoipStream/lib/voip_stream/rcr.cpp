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

#include "constants.hpp"
#include "rcr.hpp"
#include "util/log.hpp"

namespace voip_stream {

RCR::RCR()
    : filter_(RCR_NUM_ELEMENTS, RCR_BUCKETS_PER_ELEMENT, RCR_BETA, RCR_BUCKETS_PER_WORD)
{}

void RCR::operator ()(const PreRCR::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("rcr::tuple " << tuple);

    if (tuple.cdr.call_established) {
        // default stream
        if (tuple.key == tuple.cdr.calling_number) {
            filter_.add(tuple.cdr.called_number, 1, tuple.cdr.answer_timestamp);
        }
        // backup stream
        else {
            double rcr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

            FilterTuple result;
            result.ts = tuple.ts;
            result.source = SOURCE_RCR;
            result.rate = rcr;
            result.cdr = tuple.cdr;
            shipper.push(std::move(result));
        }
    }
}

}
