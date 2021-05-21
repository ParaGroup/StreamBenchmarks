#include "notification_tuple.hpp"
#include <ostream>

namespace linear_road {

std::ostream &operator <<(std::ostream &os, const NotificationTuple &tuple)
{
    return os << " vid=" << tuple.tn.vid
              << " speed=" << tuple.tn.speed
              << " toll=" << tuple.tn.toll
              << " ...";
}

}
