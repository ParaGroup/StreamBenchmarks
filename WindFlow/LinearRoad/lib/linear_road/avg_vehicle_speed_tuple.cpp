#include "avg_vehicle_speed_tuple.hpp"
#include <ostream>

namespace linear_road {

std::ostream &operator <<(std::ostream &os, const AvgVehicleSpeedTuple &tuple)
{
    return os << tuple.ts
              << " vid=" << tuple.vid
              << " minute=" << tuple.minute
              << " xway=" << tuple.xway
              << " segment=" << tuple.segment
              << " direction=" << tuple.direction
              << " avg_speed=" << tuple.avg_speed
              << " ...";
}

}
