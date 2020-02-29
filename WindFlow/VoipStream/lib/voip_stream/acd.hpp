#pragma once

#include "filter_tuple.hpp"
#include "scorer_map.hpp"
#include <windflow.hpp>

namespace voip_stream {

class ACD
{
public:

    ACD();

    void operator ()(const FilterTuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc);

private:

    double avg_;
    ScorerMap scorer_map_;
};

}
