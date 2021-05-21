#include "segment_identifier.hpp"

namespace linear_road {

bool SegmentIdentifier::operator ==(const SegmentIdentifier &other) const
{
    return xway == other.xway &&
        segment == other.segment &&
        direction == other.direction;
}

}
