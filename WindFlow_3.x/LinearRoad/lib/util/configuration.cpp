#include "configuration.hpp"
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>

namespace util {

Configuration::Configuration(const std::string &path, size_t _batch_size)
{
    std::ifstream ifs(path);
    rapidjson::IStreamWrapper stream(ifs);
    configuration_.ParseStream(stream);
    batch_size = _batch_size;
}

const rapidjson::Document &Configuration::get_tree() const
{
    return configuration_;
}

Configuration Configuration::from_args(int argc, char *argv[])
{
    if (argc != 4) {
        std::cerr << "Parameters: --batch <size> <configuration_json_file>\n";
        std::exit(1);
    }

    const char *path = argv[3];
    size_t batch_size = atoi(argv[2]);
    return Configuration(path, batch_size);
}

}
