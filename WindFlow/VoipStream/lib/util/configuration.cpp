#include "configuration.hpp"
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>

namespace util {

Configuration::Configuration(const std::string &path)
{
    std::ifstream ifs(path);
    rapidjson::IStreamWrapper stream(ifs);
    configuration_.ParseStream(stream);
}

const rapidjson::Document &Configuration::get_tree() const
{
    return configuration_;
}

Configuration Configuration::from_args(int argc, char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: <configuration>\n";
        std::exit(1);
    }

    const char *path = argv[1];
    return Configuration(path);
}

}
