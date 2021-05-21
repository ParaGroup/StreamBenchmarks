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

}
