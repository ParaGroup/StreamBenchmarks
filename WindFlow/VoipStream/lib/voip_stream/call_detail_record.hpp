#pragma once

#include <string>

namespace voip_stream {

struct CallDetailRecord {
    std::string calling_number;
    std::string called_number;
    long answer_timestamp;
    int call_duration;
    bool call_established;

    static CallDetailRecord parse(const std::string &string);
};

}
