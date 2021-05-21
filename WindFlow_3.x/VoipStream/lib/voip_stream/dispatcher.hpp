#pragma once

#include "bloom_filter.hpp"
#include "call_detail_record.hpp"
#include "parser.hpp"
#include <string>

namespace voip_stream {

class Dispatcher
{
public:

    struct Tuple
    {
        typedef std::string Key;

        uint64_t ts;
        bool new_callee;
        CallDetailRecord cdr;
        double ecr; // XXX used by the "reordering" variant
    };

public:

    Dispatcher();

    Dispatcher::Tuple operator ()(const Parser::Tuple &tuple, wf::RuntimeContext &rc);

private:

    void rotate_filters();

private:

    BloomFilter<std::string> detector_;
    BloomFilter<std::string> learner_;
    double cycle_threshold_;
};

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple);

}
