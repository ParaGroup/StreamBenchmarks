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

std::tuple<Parser::Tuple::Key, uint64_t, uint64_t> Parser::Tuple::getControlFields() const
{
    return {cdr.calling_number + ':' + cdr.called_number, 0, ts};
}

void Parser::Tuple::setControlFields(Parser::Tuple::Key key, uint64_t id, uint64_t ts)
{
    throw "UNREACHABLE";
}

void Parser::operator ()(const util::LineReaderSource::Tuple &tuple, Parser::Tuple &result, wf::RuntimeContext &rc)
{
    DEBUG_LOG("parser::tuple " << tuple);

    result.ts = tuple.ts;
    result.cdr = CallDetailRecord::parse(*tuple.line);
}

}
