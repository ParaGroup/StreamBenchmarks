#pragma once

#include "util/simple_tuple.hpp"

namespace linear_road {

struct LavTuple
{
    short minute_number;
    int xway;
    short segment;
    short direction;
    int lav;
    uint64_t ts;
};

std::ostream &operator <<(std::ostream &os, const LavTuple &tuple);

}
