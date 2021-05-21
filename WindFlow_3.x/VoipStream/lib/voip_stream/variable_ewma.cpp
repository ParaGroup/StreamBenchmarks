#include "variable_ewma.hpp"

namespace voip_stream{

VariableEWMA::VariableEWMA(double age)
    : decay_(2 / (age + 1))
    , average_(0)
    , count_(0)
{}

void VariableEWMA::add(double value)
{
    if (average_ < WARMUP_SAMPLES) {
        count_++;
        average_ += value;
    } else if (average_ == WARMUP_SAMPLES) {
        average_ = average_ / WARMUP_SAMPLES;
        count_++;
    } else {
        average_ = (value * decay_) + (average_ * (1 - decay_));
    }
}

double VariableEWMA::get_average() const
{
    if (average_ <= WARMUP_SAMPLES) {
        return 0.0;
    }

    return average_;
}

}
