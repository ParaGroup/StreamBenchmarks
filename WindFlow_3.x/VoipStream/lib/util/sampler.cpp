#include "sampler.hpp"
#include <windflow.hpp>
#include <iostream>

namespace util {

Sampler::Sampler(long samples_per_second)
    : samples_per_second_(samples_per_second)
    , epoch_(wf::current_time_nsecs())
    , counter_(0)
    , total_(0)
{}

void Sampler::add(double value, unsigned long timestamp)
{
    ++total_;

    // add samples according to the sample rate
    auto seconds = (timestamp - epoch_) / 1e9;
    if (samples_per_second_ == 0 || counter_ <= samples_per_second_ * seconds) {
        samples_.push_back(value);
        ++counter_;
    }
}

const std::vector<double> &Sampler::values() const
{
    return samples_;
}

long Sampler::total() const
{
    return total_;
}

}
