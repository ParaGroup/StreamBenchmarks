#pragma once

#include "toll_notification.hpp"
#include "util/simple_tuple.hpp"

namespace linear_road {

struct NotificationTuple : util::SimpleTuple
{
    TollNotification tn;
};

std::ostream &operator <<(std::ostream &os, const NotificationTuple &tuple);

}
