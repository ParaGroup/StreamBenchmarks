#pragma once

#include "call_detail_record.hpp"
#include "util/line_reader_source.hpp"
#include <string>

namespace voip_stream {

class Parser
{
public:

    struct Tuple
    {
        typedef std::string Key;

        uint64_t ts;
        CallDetailRecord cdr;

        std::tuple<Key, uint64_t, uint64_t> getControlFields() const;
        void setControlFields(Key key, uint64_t id, uint64_t ts);
    };

public:

    void operator ()(const util::LineReaderSource::Tuple &tuple, Parser::Tuple &result, wf::RuntimeContext &rc);
};

std::ostream &operator <<(std::ostream &os, const Parser::Tuple &tuple);

}
