#include "constants.hpp"
#include "ct24.hpp"
#include "util/log.hpp"

namespace voip_stream {

CT24::CT24()
    : filter_(CT24_NUM_ELEMENTS, CT24_BUCKETS_PER_ELEMENT, CT24_BETA, CT24_BUCKETS_PER_WORD)
{}

void CT24::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ct24::tuple " << tuple);

    if (tuple.cdr.call_established && tuple.new_callee) {
        filter_.add(tuple.cdr.calling_number, tuple.cdr.call_duration, tuple.cdr.answer_timestamp);
        double call_time = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        FilterTuple result;
        result.ts = tuple.ts;
        result.source = SOURCE_CT24;
        result.rate = call_time;
        result.cdr = tuple.cdr;
        shipper.push(result);
    }
}

}
