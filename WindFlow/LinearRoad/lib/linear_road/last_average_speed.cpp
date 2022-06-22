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

#include "last_average_speed.hpp"
#include "util/log.hpp"

namespace linear_road {

void LastAverageSpeed::operator ()(const AvgVehicleSpeedTuple &tuple, wf::Shipper<LavTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("last_average_speed::tuple" << tuple);

    short minute_number = tuple.minute;
    short m = minute_number;

    segment_identifier_.xway = tuple.xway;
    segment_identifier_.segment = tuple.segment;
    segment_identifier_.direction = tuple.direction;

    auto &latest_avg_speeds = average_speeds_per_segment_[segment_identifier_];
    auto &latest_minute_number = minute_numbers_per_segment_[segment_identifier_];
    latest_avg_speeds.push_back(tuple.avg_speed);
    latest_minute_number.push_back(minute_number);

    // discard all values that are more than 5 minutes older than current minute
    while (latest_avg_speeds.size() > 1) {
        if (latest_minute_number.front() < m - 4) {
            latest_avg_speeds.pop_front();
            latest_minute_number.pop_front();
        } else {
            break;
        }
    }

    int lav = compute_lav_value(latest_avg_speeds);

    LavTuple lav_tuple;
    lav_tuple.ts = tuple.ts;
    lav_tuple.minute_number = m + 1;
    lav_tuple.xway = segment_identifier_.xway;
    lav_tuple.segment = segment_identifier_.segment;
    lav_tuple.direction = segment_identifier_.direction;
    lav_tuple.lav = lav;
    shipper.push(std::move(lav_tuple));
}

int LastAverageSpeed::compute_lav_value(std::deque<int> &latest_avg_speeds) const
{
    int speed_sum = 0;
    int value_count = 0;
    for (int speed : latest_avg_speeds) {
        speed_sum += speed;
        ++value_count;
        if (value_count > 10) {//workaround to ensure constant workload.
            break;
        }
    }

    return speed_sum / value_count;
}

}
