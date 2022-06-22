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

#include "car_count.hpp"
#include "count_tuple.hpp"
#include "dispatcher.hpp"
#include "segment_identifier.hpp"
#include <unordered_map>
#include <windflow.hpp>

namespace linear_road {

class CountVehicles
{
public:

    void operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<CountTuple> &shipper, wf::RuntimeContext &rc);

private:

    SegmentIdentifier segment_;
    std::unordered_map<SegmentIdentifier, CarCount> counts_map_;
    short current_minute_ = -1;
};

}
