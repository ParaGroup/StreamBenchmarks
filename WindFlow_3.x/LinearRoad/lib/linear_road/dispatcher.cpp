#include "dispatcher.hpp"
#include "util/log.hpp"
#include <charconv>
#include <sstream>

namespace linear_road {

std::ostream &operator <<(std::ostream &os, const Dispatcher::Tuple &tuple)
{
    return os << " type=" << tuple.pr.type
              << " position=" << tuple.pr.position
              << " ...";
}

void Dispatcher::operator ()(const util::LineReaderSource::Tuple &tuple, wf::Shipper<Dispatcher::Tuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("dispatcher::tuple" << tuple);

    // skip tuples of type != 0
    if (tuple.line->front() != '0') {
        return;
    }

    // read tokens and convert (XXX assume always correct)
    Dispatcher::Tuple output;
    output.ts = tuple.ts;
    for (std::size_t index = 0, start = 0, end = 0;
         start < tuple.line->size();
         start = end + 1, ++index)
    {
        if ((end = tuple.line->find(',', start)) == std::string::npos) {
            end = tuple.line->size();
        }

        auto first = tuple.line->data() + start;
        auto last = tuple.line->data() + end;

        switch (index) {
        case 0: std::from_chars(first, last, output.pr.type); break;
        case 1: std::from_chars(first, last, output.pr.time); break;
        case 2: std::from_chars(first, last, output.pr.vid); break;
        case 3: std::from_chars(first, last, output.pr.speed); break;
        case 4: std::from_chars(first, last, output.pr.xway); break;
        case 5: std::from_chars(first, last, output.pr.lane); break;
        case 6: std::from_chars(first, last, output.pr.direction); break;
        case 7: std::from_chars(first, last, output.pr.segment); break;
        case 8: std::from_chars(first, last, output.pr.position); break;
        }
    }

    // finalize and send the tuple
    shipper.push(std::move(output));
}

}
