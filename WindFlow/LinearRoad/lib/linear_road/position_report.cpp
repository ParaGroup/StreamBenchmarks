#include "constants.hpp"
#include "position_report.hpp"

namespace linear_road {

PositionReport::PositionReport()
    : type(-1)
    , time(-1)
    , vid(-1)
    , speed(-1)
    , xway(-1)
    , lane(-1)
    , direction(-1)
    , segment(-1)
    , position(-1)
{}

bool PositionReport::is_on_exit_lane() const
{
    return lane == EXIT_LANE;
}

short PositionReport::get_minute_number() const
{
    return (time / 60) + 1;
}

}
