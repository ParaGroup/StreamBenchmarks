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
#include "global_acd.hpp"
#include "util/log.hpp"

namespace voip_stream {

GlobalACD::GlobalACD()
    : avg_call_duration_(ACD_DECAY_FACTOR)
{}

FilterTuple GlobalACD::operator ()(const Dispatcher::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("global_acd::tuple " << tuple);

    avg_call_duration_.add(tuple.cdr.call_duration);

    FilterTuple result;
    result.ts = tuple.ts;
    result.source = SOURCE_GlobalACD;
    result.rate = avg_call_duration_.get_average();
    result.cdr = tuple.cdr;
    return result;
}

}
