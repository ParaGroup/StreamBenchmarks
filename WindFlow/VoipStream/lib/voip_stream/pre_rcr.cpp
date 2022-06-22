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

#include "pre_rcr.hpp"
#include "util/log.hpp"
#include <sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const PreRCR::Tuple &tuple)
{
    return os << tuple.ts
              << " key=" << tuple.key
              << " new_callee=" << tuple.new_callee
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}

void PreRCR::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<PreRCR::Tuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("pre_rcr::tuple " << tuple);

    Tuple result;
    result.ts = tuple.ts;
    result.new_callee = tuple.new_callee;
    result.cdr = tuple.cdr;

    // emits the tuples twice, the key is calling then called number
    result.key.assign(result.cdr.calling_number);
    shipper.push(result);

    result.key.assign(result.cdr.called_number);
    shipper.push(std::move(result));
}

}
