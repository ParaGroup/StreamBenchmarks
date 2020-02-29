#pragma once

#include <string>

namespace voip_stream {

int murmur_hash(const std::string &bytes, int seed);

}
