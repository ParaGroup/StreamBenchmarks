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

#include "avg_vehicle_speed_tuple.hpp"
#include "lav_tuple.hpp"
#include "segment_identifier.hpp"
#include <deque>
#include <unordered_map>
#include <windflow.hpp>

namespace linear_road {

class LastAverageSpeed
{
public:

    void operator ()(const AvgVehicleSpeedTuple &tuple, wf::Shipper<LavTuple> &shipper, wf::RuntimeContext &rc);

private:

    int compute_lav_value(std::deque<int> &latest_avg_speeds) const;

private:

    std::unordered_map<SegmentIdentifier, std::deque<int>> average_speeds_per_segment_;
    std::unordered_map<SegmentIdentifier, std::deque<short>> minute_numbers_per_segment_;
    SegmentIdentifier segment_identifier_;
};

}
