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

    FilterTuple operator ()(const Dispatcher::Tuple &tuple, wf::RuntimeContext &rc);

private:

    VariableEWMA avg_call_duration_;
};

}
