#pragma once

#include <rapidjson/document.h>

namespace util {

class Configuration
{
public:

    Configuration(const std::string &path);

    const rapidjson::Document &get_tree() const;

public:

    static Configuration from_args(int argc, char *argv[]);

private:

    rapidjson::Document configuration_;
};


}
