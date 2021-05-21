#pragma once

#include "util/simple_tuple.hpp"

namespace linear_road {

struct CountTuple
{
    short minute_number;
    int xway;
    short segment;
    short direction;
    int count;
    uint64_t ts;

    CountTuple();
    bool is_progress_tuple() const;
};

std::ostream &operator <<(std::ostream &os, const CountTuple &tuple);

}
