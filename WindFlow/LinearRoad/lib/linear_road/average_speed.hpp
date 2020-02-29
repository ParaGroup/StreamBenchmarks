#pragma once

#include "avg_vehicle_speed_tuple.hpp"
#include "avg_value.hpp"
#include "dispatcher.hpp"
#include "segment_identifier.hpp"
#include <unordered_map>
#include <windflow.hpp>

namespace linear_road {

class AverageSpeed
{
public:

    void operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<AvgVehicleSpeedTuple> &shipper, wf::RuntimeContext &rc);

private:

    SegmentIdentifier segment_;
    std::unordered_map<int, std::pair<AvgValue, SegmentIdentifier>> avg_speeds_map_;
    short current_minute_ = 1;
};

}
