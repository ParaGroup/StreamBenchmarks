#include "filter_tuple.hpp"
#include <ostream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const FilterTuple &tuple)
{
    return os << tuple.ts
              << " source=" << tuple.source
              << " rate=" << tuple.rate
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}

std::tuple<FilterTuple::Key, uint64_t, uint64_t> FilterTuple::getControlFields() const
{
    return {cdr.calling_number, 0, ts};
}

void FilterTuple::setControlFields(FilterTuple::Key key, uint64_t id, uint64_t ts)
{
    throw "UNREACHABLE";
}

}
