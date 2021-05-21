#pragma once

#include "sampler.hpp"
#include "simple_tuple.hpp"
#include <ostream>
#include <string>
#include <vector>
#include <windflow.hpp>

namespace util {

class LineReaderSource
{
public:

    struct Tuple : util::SimpleTuple
    {
        const std::string *line;
    };

public:

    LineReaderSource(int run_time_sec, int gen_rate, const std::string &path);

    void operator ()(wf::Source_Shipper<LineReaderSource::Tuple> &shipper, wf::RuntimeContext &rc);

    void active_delay(unsigned long waste_time);

private:

    void read_all(const std::string &path);

private:

    const int run_time_sec_;
    const int gen_rate_;
    util::Sampler throughput_;
    std::vector<std::string> data_;
    std::size_t index_;
    std::size_t counter_;
    long epoch_;
};

std::ostream &operator <<(std::ostream &os, const LineReaderSource::Tuple &tuple);

}
