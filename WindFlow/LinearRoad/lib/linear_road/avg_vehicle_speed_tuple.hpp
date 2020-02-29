#pragma once

#include "util/simple_tuple.hpp"

namespace linear_road {

struct AvgVehicleSpeedTuple : util::SimpleTuple
{
    int vid;
    short minute;
    int xway;
    short segment;
    short direction;
    int avg_speed;
};

std::ostream &operator <<(std::ostream &os, const AvgVehicleSpeedTuple &tuple);

}
