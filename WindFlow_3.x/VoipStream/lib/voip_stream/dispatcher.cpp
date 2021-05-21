#include "constants.hpp"
#include "dispatcher.hpp"
#include "util/log.hpp"
//#include <charconv>
#include <cmath>
#include <sstream>

namespace voip_stream {

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple)
{
    return os << tuple.ts
              << " new_callee=" << tuple.new_callee
              << " cdr.calling_number=" << tuple.cdr.calling_number
              << " cdr.called_number=" << tuple.cdr.called_number
              << " cdr.answer_timestamp=" << tuple.cdr.answer_timestamp
              << " ...";
}

Dispatcher::Dispatcher()
    : detector_(VAR_DETECT_ERROR_RATE, VAR_DETECT_APROX_SIZE)
    , learner_(VAR_DETECT_ERROR_RATE, VAR_DETECT_APROX_SIZE)
{
    cycle_threshold_ = detector_.size() / std::sqrt(2);
}

Dispatcher::Tuple Dispatcher::operator ()(const Parser::Tuple &tuple, wf::RuntimeContext &rc)
{
    DEBUG_LOG("dispatcher::tuple " << tuple);

    const auto key = tuple.cdr.calling_number + ':' + tuple.cdr.called_number;
    bool new_callee = false;

    // add pair to learner
    learner_.add(key);

    // check if the pair exists
    // if not, add to the detector
    if (!detector_.membership_test(key)) {
        detector_.add(key);
        new_callee = true;
    }

    // if number of non-zero bits is above threshold, rotate filters
    if (detector_.get_num_non_zero() > cycle_threshold_) {
        rotate_filters();
    }

    // fill in the result
    Dispatcher::Tuple result;
    result.ts = tuple.ts;
    result.new_callee = new_callee;
    result.cdr = tuple.cdr;
    return result;
}

void Dispatcher::rotate_filters()
{
    // TODO implement move constructor?
    std::swap(detector_, learner_);
    learner_.clear();
}

}
