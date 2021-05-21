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
