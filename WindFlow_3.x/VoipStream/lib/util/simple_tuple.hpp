#pragma once

#include <cstdint>
#include <tuple>

namespace util {

struct SimpleTuple
{
    uint64_t ts;

    std::tuple<int, uint64_t, uint64_t> getControlFields() const;
    void setControlFields(int key, uint64_t id, uint64_t ts);
};

}
