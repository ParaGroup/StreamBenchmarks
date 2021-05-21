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

    struct Tuple
    {
        const std::string *line;
        uint64_t ts;
    };

public:

    LineReaderSource(int run_time_sec, int gen_rate, const std::string &path);

    void operator ()(wf::Source_Shipper<Tuple> &shipper, wf::RuntimeContext &rc);

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

   	void active_delay(unsigned long waste_time);
};

	std::ostream &operator <<(std::ostream &os, const LineReaderSource::Tuple &tuple);
}
