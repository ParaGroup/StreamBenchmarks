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
