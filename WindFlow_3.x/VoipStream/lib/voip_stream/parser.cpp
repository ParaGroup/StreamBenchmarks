#include "parser.hpp"
#include "util/log.hpp"
//#include <charconv>
#include <sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const Parser::Tuple &tuple)
{
    return os << tuple.ts
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " cdr.call_established=" << tuple.cdr.call_established
              << " ...";
}

Parser::Tuple Parser::operator ()(const util::LineReaderSource::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("parser::tuple " << tuple);

    Parser::Tuple result;
    result.ts = tuple.ts;
    result.cdr = CallDetailRecord::parse(*tuple.line);
    return result;
}

}
