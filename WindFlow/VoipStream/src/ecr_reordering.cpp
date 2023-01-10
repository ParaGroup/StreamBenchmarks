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
#include<voip_stream/ecr_reordering.hpp>
#include<util/log.hpp>

namespace voip_stream { namespace reordering {

ECR::ECR()
    : filter_(ECR_NUM_ELEMENTS, ECR_BUCKETS_PER_ELEMENT, ECR_BETA, ECR_BUCKETS_PER_WORD)
{}

void ECR::operator ()(Dispatcher::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ecr_reordering::tuple " << tuple);

    if (tuple.cdr.call_established) {
        filter_.add(tuple.cdr.calling_number, 1, tuple.cdr.answer_timestamp);
        double ecr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        tuple.ecr = ecr;
    } else {
        tuple.ecr = -1;
    }
}

}}
