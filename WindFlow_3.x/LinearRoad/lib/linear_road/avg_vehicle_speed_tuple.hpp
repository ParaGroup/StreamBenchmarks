#pragma once

#include "util/simple_tuple.hpp"

namespace linear_road {

struct AvgVehicleSpeedTuple
{
    int vid;
    short minute;
    int xway;
    short segment;
    short direction;
    int avg_speed;
    uint64_t ts;
};

std::ostream &operator <<(std::ostream &os, const AvgVehicleSpeedTuple &tuple);

}
