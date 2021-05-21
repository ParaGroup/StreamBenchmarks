#include "toll_notification_las.hpp"
#include "util/log.hpp"

namespace linear_road {

void TollNotificationLas::operator ()(const LavTuple &tuple, wf::Shipper<NotificationTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("toll_notification_las::tuple" << tuple);

    NotificationTuple nt{};
    nt.ts = tuple.ts;
    shipper.push(std::move(nt)); // as an indication.

    check_minute(tuple.minute_number);

    SegmentIdentifier segment_identifier{tuple.xway, tuple.segment, tuple.direction};
    current_minute_lavs_.insert(std::make_pair(segment_identifier, tuple.lav));
}

void TollNotificationLas::check_minute(short minute)
{
    if (minute < current_minute_) {
        // restart...
        current_minute_ = minute;
    } else if (minute > current_minute_) {
        current_minute_ = minute;
        previous_minute_accidents_.swap(current_minute_accidents_);
        current_minute_accidents_.clear();
        previous_minute_counts_.swap(current_minute_counts_);
        current_minute_counts_.clear();
        previous_minute_lavs_.swap(current_minute_lavs_);
        current_minute_lavs_.clear();
    }
}

}
