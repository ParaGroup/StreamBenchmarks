#pragma once

#include "metric.hpp"
#include "sampler.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace util {

class MetricGroup
{
public:

    void add(std::string name, Sampler sampler);

    // XXX this consumes the groups
    void dump_all();

private:

    Metric get_metric(std::string name);

private:

    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<Sampler>> map_;
};

extern MetricGroup metric_group;

}
