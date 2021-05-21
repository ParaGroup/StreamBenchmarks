#include "avg_value.hpp"

namespace linear_road {

AvgValue::AvgValue(int initalValue)
{
    sum_ = initalValue;
    count_ = 1;
}


void AvgValue::update_average(int value)
{
    sum_ += value;
    ++count_;
}

int AvgValue::get_average() const
{
    return sum_ / count_;
}

}
