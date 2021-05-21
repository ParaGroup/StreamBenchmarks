#include "constants.hpp"
#include "toll_notification_pos.hpp"
#include "util/log.hpp"
#include <cassert>

namespace linear_road {

void TollNotificationPos::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<NotificationTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("toll_notification_pos::tuple" << tuple);

    NotificationTuple nt{};
    nt.ts = tuple.ts;
    shipper.push(std::move(nt)); // as an indication.

    check_minute(tuple.pr.get_minute_number());

    if (tuple.pr.is_on_exit_lane()) {
        auto it = last_toll_notification_.find(tuple.pr.vid);
        assert(it == last_toll_notification_.end() || it->second.pos.xway != -1);
        return;
    }

    short current_segment = tuple.pr.segment;
    int vid = tuple.pr.vid;
    auto result = all_cars_.insert(std::make_pair(vid, current_segment));
    if (result.second && current_segment == result.first->second) {
        return;
    }

    int toll = 0;

    SegmentIdentifier segment_identifier = {tuple.pr.xway, tuple.pr.segment, tuple.pr.direction};
    auto it = previous_minute_lavs_.find(segment_identifier);

    int lav, lav_value;
    if (it != previous_minute_lavs_.end()) {
        lav = it->second;
        lav_value = lav;
    } else {
        lav = 0;
        lav_value = 0;
    }

    if (lav_value < 50) {
        auto it = previous_minute_counts_.find(segment_identifier);
        int car_count = it == previous_minute_counts_.end() ? 0 : it->second;

        if (car_count > 50) {
            // downstream is either larger or smaller of current segment
            short direction = tuple.pr.direction;
            short dir = direction;
            // EASTBOUND == 0 => diff := 1
            // WESTBOUNT == 1 => diff := -1
            short diff = (short) -(dir - 1 + ((dir + 1) / 2));
            assert(dir == EASTBOUND ? diff == 1 : diff == -1);

            int xway = tuple.pr.xway;
            short cur_seg = current_segment;

            segment_to_check_.xway = xway;
            segment_to_check_.direction = direction;

            int i;
            for (i = 0; i <= 4; ++i) {
                short next_segment = cur_seg + (diff * i);
                assert(dir == EASTBOUND ? next_segment >= cur_seg : next_segment <= cur_seg);

                segment_to_check_.segment = next_segment;

                if (previous_minute_accidents_.find(segment_to_check_) != previous_minute_accidents_.end()) {
                    break;
                }
            }

            if (i == 5) { // only true if no accident was found and "break" was not executed
                int var = car_count - 50;
                toll = 2 * var * var;
            }
        }
    }

    TollNotification toll_notification{vid, lav, toll, tuple.pr};

    auto last_notification_it = last_toll_notification_.find(vid);
    assert(last_notification_it == last_toll_notification_.end() || (last_notification_it->second.pos.xway != -1));
    assert(toll_notification.pos.xway != -1);
}

void TollNotificationPos::check_minute(short minute)
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
