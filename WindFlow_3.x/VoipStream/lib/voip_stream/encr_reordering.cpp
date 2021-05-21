#include "constants.hpp"
#include "encr_reordering.hpp"
#include "util/log.hpp"

namespace voip_stream { namespace reordering {

ENCR::ENCR()
    : filter_(ENCR_NUM_ELEMENTS, ENCR_BUCKETS_PER_ELEMENT, ENCR_BETA, ENCR_BUCKETS_PER_WORD)
{}

void ENCR::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("encr_reordering::tuple " << tuple);

    if (tuple.cdr.call_established) {

        // forward the tuple from ECR
        if (tuple.ecr >= 0) {
            FilterTuple result;
            result.ts = tuple.ts;
            result.source = SOURCE_ECR;
            result.rate = tuple.ecr;
            result.cdr = tuple.cdr;
            shipper.push(std::move(result));
        }

        if (tuple.new_callee) {
            filter_.add(tuple.cdr.calling_number, 1, tuple.cdr.answer_timestamp);
            double rate = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

            FilterTuple result;
            result.ts = tuple.ts;
            result.source = SOURCE_ENCR;
            result.rate = rate;
            result.cdr = tuple.cdr;
            shipper.push(std::move(result));
        }
    }
}

}}
