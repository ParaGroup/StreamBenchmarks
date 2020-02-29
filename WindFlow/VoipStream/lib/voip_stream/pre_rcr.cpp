#include "pre_rcr.hpp"
#include "util/log.hpp"
#include <sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const PreRCR::Tuple &tuple)
{
    return os << tuple.ts
              << " key=" << tuple.key
              << " new_callee=" << tuple.new_callee
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}

std::tuple<PreRCR::Tuple::Key, uint64_t, uint64_t> PreRCR::Tuple::getControlFields() const
{
    return {key, 0, ts};
}

void PreRCR::Tuple::setControlFields(PreRCR::Tuple::Key key, uint64_t id, uint64_t ts)
{
    throw "UNREACHABLE";
}

void PreRCR::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<PreRCR::Tuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("pre_rcr::tuple " << tuple);

    Tuple result;
    result.ts = tuple.ts;
    result.new_callee = tuple.new_callee;
    result.cdr = tuple.cdr;

    // emits the tuples twice, the key is calling then called number
    result.key.assign(result.cdr.calling_number);
    shipper.push(result);

    result.key.assign(result.cdr.called_number);
    shipper.push(result);
}

}
