/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#include<linear_road/toll_notification_las.hpp>
#include<util/log.hpp>

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
