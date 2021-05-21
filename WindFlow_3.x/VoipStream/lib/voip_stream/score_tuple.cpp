#include "score_tuple.hpp"
#include <ostream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const ScoreTuple &tuple)
{
    return os << tuple.ts
              << " score=" << tuple.score
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}


}
