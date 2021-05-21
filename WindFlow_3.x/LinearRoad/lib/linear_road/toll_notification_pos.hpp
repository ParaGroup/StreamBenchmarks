#pragma once

#include "dispatcher.hpp"
#include "notification_tuple.hpp"
#include "segment_identifier.hpp"
#include <unordered_map>
#include <unordered_set>
#include <windflow.hpp>

namespace linear_road {

class TollNotificationPos
{
public:

    void operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<NotificationTuple> &shipper, wf::RuntimeContext &rc);

private:

    void check_minute(short minute);

private:

    std::unordered_map<int, short> all_cars_;
    std::unordered_map<int, TollNotification> last_toll_notification_;
    SegmentIdentifier segment_to_check_;
    std::unordered_set<SegmentIdentifier> current_minute_accidents_;
    std::unordered_set<SegmentIdentifier> previous_minute_accidents_;
    std::unordered_map<SegmentIdentifier, int> current_minute_counts_;
    std::unordered_map<SegmentIdentifier, int> previous_minute_counts_;
    std::unordered_map<SegmentIdentifier, int> current_minute_lavs_;
    std::unordered_map<SegmentIdentifier, int> previous_minute_lavs_;
    int current_minute_ = -1;
};

}
