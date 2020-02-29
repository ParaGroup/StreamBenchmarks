#include "constants.hpp"
#include "ecr24.hpp"
#include "util/log.hpp"

namespace voip_stream {

ECR24::ECR24()
    : filter_(ECR24_NUM_ELEMENTS, ECR24_BUCKETS_PER_ELEMENT, ECR24_BETA, ECR24_BUCKETS_PER_WORD)
{}

void ECR24::operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ecr24::tuple " << tuple);

    if (tuple.cdr.call_established) {
        filter_.add(tuple.cdr.calling_number, 1, tuple.cdr.answer_timestamp);
        double ecr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        FilterTuple result;
        result.ts = tuple.ts;
        result.source = SOURCE_ECR24;
        result.rate = ecr;
        result.cdr = tuple.cdr;
        shipper.push(result);
    }
}

}
