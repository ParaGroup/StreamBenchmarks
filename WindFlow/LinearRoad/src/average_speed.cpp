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

#include<linear_road/average_speed.hpp>
#include<util/log.hpp>

namespace linear_road {

void AverageSpeed::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<AvgVehicleSpeedTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("average_speed::tuple" << tuple);

    PositionReport input_position_report = tuple.pr;

    int vid = input_position_report.vid;
    short minute = input_position_report.get_minute_number();
    int speed = input_position_report.speed;
    segment_.xway = input_position_report.xway;
    segment_.segment = input_position_report.segment;
    segment_.direction = input_position_report.direction;


    for (auto it : avg_speeds_map_) {
        SegmentIdentifier seg_id = it.second.second;

        AvgVehicleSpeedTuple avg_vehicle_speed_tuple;
        //avg_vehicle_speed_tuple.ts = tuple.ts;
        avg_vehicle_speed_tuple.vid = it.first;
        avg_vehicle_speed_tuple.minute = current_minute_;
        avg_vehicle_speed_tuple.xway = seg_id.xway;
        avg_vehicle_speed_tuple.segment = seg_id.segment;
        avg_vehicle_speed_tuple.direction = seg_id.direction;
        avg_vehicle_speed_tuple.avg_speed = it.second.first.get_average();
        avg_vehicle_speed_tuple.ts = tuple.ts;
        shipper.push(std::move(avg_vehicle_speed_tuple));
    }

    avg_speeds_map_.clear();
    current_minute_ = minute;

    auto it = avg_speeds_map_.find(vid);
    bool emitted = false;
    if (it != avg_speeds_map_.end() && !(it->second.second == segment_)) {
        SegmentIdentifier seg_id = it->second.second;

        AvgVehicleSpeedTuple avg_vehicle_speed_tuple;
        //avg_vehicle_speed_tuple.ts = tuple.ts;
        avg_vehicle_speed_tuple.vid = vid;
        avg_vehicle_speed_tuple.minute = current_minute_;
        avg_vehicle_speed_tuple.xway = seg_id.xway;
        avg_vehicle_speed_tuple.segment = seg_id.segment;
        avg_vehicle_speed_tuple.direction = seg_id.direction;
        avg_vehicle_speed_tuple.avg_speed = it->second.first.get_average();
        avg_vehicle_speed_tuple.ts = tuple.ts;
        shipper.push(std::move(avg_vehicle_speed_tuple));

        emitted = true;
    }

    if (emitted || it == avg_speeds_map_.end()) {
        avg_speeds_map_.insert(std::make_pair(vid, std::make_pair(AvgValue(speed), segment_)));
    } else {
        it->second.first.update_average(speed);
    }
}

}
