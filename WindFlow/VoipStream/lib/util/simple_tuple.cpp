#include "simple_tuple.hpp"

namespace util {

std::tuple<int, uint64_t, uint64_t> SimpleTuple::getControlFields() const
{
    return {0, 0, ts};
}

void SimpleTuple::setControlFields(int key, uint64_t id, uint64_t ts)
{
    throw "UNREACHABLE";
}

}
