#include "constants.hpp"
#include "ecr.hpp"
#include "util/log.hpp"

namespace voip_stream {

ECR::ECR()
    : filter_(ECR_NUM_ELEMENTS, ECR_BUCKETS_PER_ELEMENT, ECR_BETA, ECR_BUCKETS_PER_WORD)
{}

void ECR::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ecr::tuple " << tuple);

    if (tuple.cdr.call_established) {
        filter_.add(tuple.cdr.calling_number, 1, tuple.cdr.answer_timestamp);
        double ecr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        FilterTuple result;
        result.ts = tuple.ts;
        result.source = SOURCE_ECR;
        result.rate = ecr;
        result.cdr = tuple.cdr;
        shipper.push(std::move(result));
    }
}

}
