#pragma once

#include "filter_tuple.hpp"
#include "scorer_map.hpp"
#include <windflow.hpp>

namespace voip_stream {

class FoFiR
{
public:

    FoFiR();

    void operator ()(const FilterTuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc);

private:

    ScorerMap scorer_map_;
};

}
