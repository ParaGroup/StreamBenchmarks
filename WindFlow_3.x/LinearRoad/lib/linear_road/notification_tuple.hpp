#pragma once

#include "toll_notification.hpp"
#include "util/simple_tuple.hpp"

namespace linear_road {

struct NotificationTuple
{
    TollNotification tn;
    uint64_t ts;
};

std::ostream &operator <<(std::ostream &os, const NotificationTuple &tuple);

}
