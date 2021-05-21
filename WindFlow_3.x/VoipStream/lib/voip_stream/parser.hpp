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
    };

public:

    Parser::Tuple operator ()(const util::LineReaderSource::Tuple &tuple, wf::RuntimeContext &rc);
};

std::ostream &operator <<(std::ostream &os, const Parser::Tuple &tuple);

}
