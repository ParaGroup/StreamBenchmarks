#include "count_tuple.hpp"
#include <ostream>

namespace linear_road {

CountTuple::CountTuple()
    : minute_number(-1)
    , xway(-1)
    , segment(-1)
    , direction(-1)
    , count(-1)
{}

bool CountTuple::is_progress_tuple() const
{
    return xway == -1;
}

std::ostream &operator <<(std::ostream &os, const CountTuple &tuple)
{
    return os << tuple.ts
              << " minute_number=" << tuple.minute_number
              << " xway=" << tuple.xway
              << " segment=" << tuple.segment
              << " direction=" << tuple.direction
              << " count=" << tuple.count
              << " ...";
}

}
