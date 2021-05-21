#include "constants.hpp"
#include "rcr.hpp"
#include "util/log.hpp"

namespace voip_stream {

RCR::RCR()
    : filter_(RCR_NUM_ELEMENTS, RCR_BUCKETS_PER_ELEMENT, RCR_BETA, RCR_BUCKETS_PER_WORD)
{}

void RCR::operator ()(const PreRCR::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("rcr::tuple " << tuple);

    if (tuple.cdr.call_established) {
        // default stream
        if (tuple.key == tuple.cdr.calling_number) {
            filter_.add(tuple.cdr.called_number, 1, tuple.cdr.answer_timestamp);
        }
        // backup stream
        else {
            double rcr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

            FilterTuple result;
            result.ts = tuple.ts;
            result.source = SOURCE_RCR;
            result.rate = rcr;
            result.cdr = tuple.cdr;
            shipper.push(std::move(result));
        }
    }
}

}
