#pragma once

#include "log.hpp"
#include "metric_group.hpp"
#include <windflow.hpp>

namespace util {

template <typename Tuple>
class DrainSink
{
public:

    DrainSink(long sampling_rate)
        : latency_(sampling_rate)
    {}

    void operator ()(std::optional<Tuple> &tuple, wf::RuntimeContext &rc)
    {
        if (tuple) {
            DEBUG_LOG("sink::tuple " << *tuple);

            // compute the latency for this tuple
            auto timestamp = wf::current_time_nsecs();
            auto latency = (timestamp - tuple->ts) / 1e3; // microseconds
            latency_.add(latency, timestamp);
        } else {
            DEBUG_LOG("sink::finished");

            // end of stream, dump metrics
            util::metric_group.add("latency", latency_);
        }
    }

private:

    util::Sampler latency_;
};

}
