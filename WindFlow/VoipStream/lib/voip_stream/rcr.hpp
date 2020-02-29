#pragma once

#include "pre_rcr.hpp"
#include "filter_tuple.hpp"
#include "odtd_bloom_filter.hpp"
#include <windflow.hpp>

namespace voip_stream {

class RCR
{
public:

    RCR();

    void operator ()(const PreRCR::Tuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc);

private:

    ODTDBloomFilter filter_;
};

}
