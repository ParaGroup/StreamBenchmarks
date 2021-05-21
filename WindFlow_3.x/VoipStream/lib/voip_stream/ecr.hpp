#pragma once

#include "dispatcher.hpp"
#include "filter_tuple.hpp"
#include "odtd_bloom_filter.hpp"
#include <windflow.hpp>

namespace voip_stream {

class ECR
{
public:

    ECR();

    void operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc);

private:

    ODTDBloomFilter filter_;
};

}
