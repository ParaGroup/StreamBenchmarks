#pragma once

#include <string>
#include <vector>

namespace util {

class Metric
{
public:

    Metric(const std::string &name);

    void add(double value);

    void total(long total);

    void dump();

private:

    std::string name_;
    std::vector<double> samples_;
    long total_;
};

}
