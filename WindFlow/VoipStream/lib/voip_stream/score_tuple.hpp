#pragma once

#include "call_detail_record.hpp"
#include <string>
#include <tuple>

namespace voip_stream {

struct ScoreTuple
{
    typedef std::string Key;

    uint64_t ts;
    double score;
    CallDetailRecord cdr;

    std::tuple<Key, uint64_t, uint64_t> getControlFields() const;
    void setControlFields(Key key, uint64_t id, uint64_t ts);
};

std::ostream &operator <<(std::ostream &os, const ScoreTuple &tuple);

}
