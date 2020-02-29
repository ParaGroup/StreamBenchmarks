#pragma once

#include "util/simple_tuple.hpp"

namespace linear_road {

struct LavTuple : util::SimpleTuple
{
    short minute_number;
    int xway;
    short segment;
    short direction;
    int lav;
};

std::ostream &operator <<(std::ostream &os, const LavTuple &tuple);

}
