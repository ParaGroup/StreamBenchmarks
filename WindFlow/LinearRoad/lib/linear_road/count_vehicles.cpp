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
            shipper.push(count_tuple);
        }
    }
    if (!emitted) {
        CountTuple count_tuple;
        count_tuple.ts = tuple.ts;
        count_tuple.minute_number = current_minute_;
        shipper.push(count_tuple);
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
