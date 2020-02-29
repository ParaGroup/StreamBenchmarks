#pragma once

#include <functional>

namespace linear_road {

struct SegmentIdentifier {
    int xway;
    short segment;
    short direction;

    bool operator ==(const SegmentIdentifier &other) const;
};

}

namespace std {

template<>
struct hash<linear_road::SegmentIdentifier>
{
    size_t operator ()(const linear_road::SegmentIdentifier &object) const
    {
        return hash<int>()(object.xway) ^
            hash<int>()(object.segment) ^
            hash<int>()(object.direction);
    }
};

}
