#pragma once

#include "dispatcher.hpp"
#include <windflow.hpp>

namespace voip_stream {

class PreRCR
{
public:

    struct Tuple
    {
        typedef std::string Key;
        Key key;

        uint64_t ts;
        bool new_callee;
        CallDetailRecord cdr;

        std::tuple<Key, uint64_t, uint64_t> getControlFields() const;
        void setControlFields(Key key, uint64_t id, uint64_t ts);
    };

public:

    void operator ()(const Dispatcher::Tuple &tuple, wf::Shipper<Tuple> &shipper, wf::RuntimeContext &rc);
};

std::ostream &operator <<(std::ostream &os, const PreRCR::Tuple &tuple);

}
