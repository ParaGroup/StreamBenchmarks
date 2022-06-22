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

#include "count_vehicles.hpp"
#include "util/log.hpp"

namespace linear_road {

void CountVehicles::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<CountTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("car_count::tuple" << tuple);

    PositionReport input_position_report = tuple.pr;

    short minute = input_position_report.get_minute_number();
    segment_.xway = input_position_report.xway;
    segment_.segment = input_position_report.segment;
    segment_.direction = input_position_report.direction;

    bool emitted = false;

    for (auto it : counts_map_) {
        SegmentIdentifier seg_id = it.first;

        int count = it.second.count;
        if (count > 50) {
            emitted = true;

            CountTuple count_tuple;
            count_tuple.ts = tuple.ts;
            count_tuple.minute_number = current_minute_;
            count_tuple.xway = seg_id.xway;
            count_tuple.segment = seg_id.segment;
            count_tuple.direction = seg_id.direction;
            count_tuple.count = count;
            shipper.push(std::move(count_tuple));
        }
    }
    if (!emitted) {
        CountTuple count_tuple;
        count_tuple.ts = tuple.ts;
        count_tuple.minute_number = current_minute_;
        shipper.push(std::move(count_tuple));
    }
    counts_map_.clear();
    current_minute_ = minute;


    auto it = counts_map_.find(segment_);
    if (it == counts_map_.end()) {
        CarCount seg_cnt;
        counts_map_.insert(std::make_pair(segment_, seg_cnt));
    } else {
        ++it->second.count;
    }
}

}
