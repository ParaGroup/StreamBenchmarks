#include "toll_notification_cv.hpp"
#include "util/log.hpp"

namespace linear_road {

void TollNotificationCv::operator ()(const CountTuple &tuple, wf::Shipper<NotificationTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("toll_notification_cv::tuple" << tuple);

    NotificationTuple nt{};
    nt.ts = tuple.ts;
    shipper.push(nt); // as an indication.

    check_minute(tuple.minute_number);

    if (tuple.is_progress_tuple()) {
        return;
    }

    SegmentIdentifier segment_identifier{tuple.xway, tuple.segment, tuple.direction};
    current_minute_counts_.insert(std::make_pair(segment_identifier, tuple.count));
}

void TollNotificationCv::check_minute(short minute)
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
