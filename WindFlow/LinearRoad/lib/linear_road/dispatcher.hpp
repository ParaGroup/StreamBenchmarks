#pragma once

#include "position_report.hpp"
#include "util/line_reader_source.hpp"
#include "util/simple_tuple.hpp"

namespace linear_road {

class Dispatcher
{
public:

    struct Tuple : util::SimpleTuple
    {
        PositionReport pr;
    };

public:

    void operator ()(const util::LineReaderSource::Tuple &tuple, wf::Shipper<Tuple> &shipper, wf::RuntimeContext &rc);
};

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple);

}
