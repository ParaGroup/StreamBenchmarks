#include "lav_tuple.hpp"
#include <ostream>

namespace linear_road {

std::ostream &operator <<(std::ostream &os, const LavTuple &tuple)
{
    return os << tuple.ts
              << " minute_number=" << tuple.minute_number
              << " xway=" << tuple.xway
              << " segment=" << tuple.segment
              << " direction=" << tuple.direction
              << " lav=" << tuple.lav
              << " ...";
}

}
