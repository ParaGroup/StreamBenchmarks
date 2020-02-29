#pragma once

#include "position_report.hpp"

namespace linear_road {

struct TollNotification {
    int vid;
    int speed;
    int toll;
    PositionReport pos;
};

}
