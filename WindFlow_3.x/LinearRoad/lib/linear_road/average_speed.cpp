#include "average_speed.hpp"
#include "util/log.hpp"

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
