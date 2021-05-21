#pragma once

#include <rapidjson/document.h>

namespace util {

class Configuration
{
public:

    Configuration(const std::string &path, size_t _batch_size);

    const rapidjson::Document &get_tree() const;

public:

    static Configuration from_args(int argc, char *argv[]);

    size_t batch_size;

private:

    rapidjson::Document configuration_;
};


}
