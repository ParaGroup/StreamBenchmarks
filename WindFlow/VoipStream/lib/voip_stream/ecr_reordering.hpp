#pragma once

#include "dispatcher.hpp"
#include "filter_tuple.hpp"
#include "odtd_bloom_filter.hpp"
#include <windflow.hpp>

namespace voip_stream { namespace reordering {

class ECR
{
public:

    ECR();

    void operator ()(Dispatcher::Tuple &tuple, wf::RuntimeContext &rc);

private:

    ODTDBloomFilter filter_;
};

}}
