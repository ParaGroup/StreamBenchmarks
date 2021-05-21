#include "constants.hpp"
#include "ecr_reordering.hpp"
#include "util/log.hpp"

namespace voip_stream { namespace reordering {

ECR::ECR()
    : filter_(ECR_NUM_ELEMENTS, ECR_BUCKETS_PER_ELEMENT, ECR_BETA, ECR_BUCKETS_PER_WORD)
{}

void ECR::operator ()(Dispatcher::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("ecr_reordering::tuple " << tuple);

    if (tuple.cdr.call_established) {
        filter_.add(tuple.cdr.calling_number, 1, tuple.cdr.answer_timestamp);
        double ecr = filter_.estimate_count(tuple.cdr.calling_number, tuple.cdr.answer_timestamp);

        tuple.ecr = ecr;
    } else {
        tuple.ecr = -1;
    }
}

}}
