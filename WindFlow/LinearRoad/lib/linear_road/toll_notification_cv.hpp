#pragma once

#include "count_tuple.hpp"
#include "notification_tuple.hpp"
#include "segment_identifier.hpp"
#include <unordered_map>
#include <unordered_set>
#include <windflow.hpp>

namespace linear_road {

class TollNotificationCv
{
public:

    void operator ()(const CountTuple &tuple, wf::Shipper<NotificationTuple> &shipper, wf::RuntimeContext &rc);

private:

    void check_minute(short minute);

private:

    std::unordered_set<SegmentIdentifier> current_minute_accidents_;
    std::unordered_set<SegmentIdentifier> previous_minute_accidents_;
    std::unordered_map<SegmentIdentifier, int> current_minute_counts_;
    std::unordered_map<SegmentIdentifier, int> previous_minute_counts_;
    std::unordered_map<SegmentIdentifier, int> current_minute_lavs_;
    std::unordered_map<SegmentIdentifier, int> previous_minute_lavs_;
    int current_minute_ = -1;
};

}
