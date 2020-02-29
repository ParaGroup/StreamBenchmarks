#pragma once

#include <vector>

namespace util {

class Sampler
{
public:

    Sampler(long samples_per_second = 0);

    void add(double value, unsigned long timestamp = 0);

    const std::vector<double> &values() const;

    long total() const;

private:

    const long samples_per_second_;
    unsigned long epoch_;
    long counter_;
    long total_;
    std::vector<double> samples_;
};

}
