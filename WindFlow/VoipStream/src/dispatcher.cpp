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
#include<voip_stream/dispatcher.hpp>
#include<util/log.hpp>
//#include <charconv>
#include<cmath>
#include<sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple)
{
    return os << tuple.ts
              << " new_callee=" << tuple.new_callee
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}

Dispatcher::Dispatcher()
    : detector_(VAR_DETECT_ERROR_RATE, VAR_DETECT_APROX_SIZE)
    , learner_(VAR_DETECT_ERROR_RATE, VAR_DETECT_APROX_SIZE)
{
    cycle_threshold_ = detector_.size() / std::sqrt(2);
}

Dispatcher::Tuple Dispatcher::operator ()(const Parser::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("dispatcher::tuple " << tuple);

    const auto key = tuple.cdr.calling_number + ':' + tuple.cdr.called_number;
    bool new_callee = false;

    // add pair to learner
    learner_.add(key);

    // check if the pair exists
    // if not, add to the detector
    if (!detector_.membership_test(key)) {
        detector_.add(key);
        new_callee = true;
    }

    // if number of non-zero bits is above threshold, rotate filters
    if (detector_.get_num_non_zero() > cycle_threshold_) {
        rotate_filters();
    }

    // fill in the result
    Dispatcher::Tuple result;
    result.ts = tuple.ts;
    result.new_callee = new_callee;
    result.cdr = tuple.cdr;
    return result;
}

void Dispatcher::rotate_filters()
{
    // TODO implement move constructor?
    std::swap(detector_, learner_);
    learner_.clear();
}

}
