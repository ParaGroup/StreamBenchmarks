#include "metric_group.hpp"
#include <algorithm>
#include <fstream>
#include <numeric>
#include <rapidjson/prettywriter.h>
#include <sstream>

using namespace rapidjson;

namespace util {

MetricGroup metric_group;

void MetricGroup::add(std::string name, Sampler sampler)
{
    // XXX this is not time critical, using a simple mutex lock is good enough
    std::lock_guard lock(mutex_);
    auto &samplers = map_[name];
    samplers.push_back(sampler);
}

void MetricGroup::dump_all()
{
    for (auto &it : map_) {
        Metric metric = get_metric(it.first);
        metric.dump();
    }
}

Metric MetricGroup::get_metric(std::string name)
{
    Metric metric(name);

    // consume all the groups
    long total = 0;
    auto &samplers = map_.at(name);
    while (!samplers.empty()) {
        auto sampler = samplers.back();

        // add all the values from the sampler
        total += sampler.total();
        for (double value : sampler.values()) {
            metric.add(value);
        }

        // discard it
        samplers.pop_back();
    }

    // set total
    metric.total(total);
    return metric;
}

}
