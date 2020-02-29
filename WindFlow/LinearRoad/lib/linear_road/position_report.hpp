#pragma once

namespace linear_road {

struct PositionReport {
    short type;
    int time;
    int vid;
    int speed;
    int xway;
    short lane;
    short direction;
    short segment;
    int position;

    PositionReport();

    bool is_on_exit_lane() const;
    short get_minute_number() const;
};

}
