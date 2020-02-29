#pragma once

#include "dispatcher.hpp"
#include "filter_tuple.hpp"
#include "variable_ewma.hpp"
#include <windflow.hpp>

namespace voip_stream {

class GlobalACD
{
public:

    GlobalACD();

    void operator ()(const Dispatcher::Tuple &tuple, FilterTuple &result, wf::RuntimeContext &rc);

private:

    VariableEWMA avg_call_duration_;
};

}
