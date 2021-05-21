#pragma once

#include "call_detail_record.hpp"
#include <string>
#include <tuple>

namespace voip_stream {

struct FilterTuple
{
    typedef std::string Key;

    uint64_t ts;
    int source;
    double rate;
    CallDetailRecord cdr;
    double ecr; // XXX used by the "reordering" variant
};

std::ostream &operator <<(std::ostream &os, const FilterTuple &tuple);

}
