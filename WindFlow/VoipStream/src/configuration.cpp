/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#include<util/configuration.hpp>
#include<cstdlib>
#include<fstream>
#include<iostream>
#include<rapidjson/document.h>
#include<rapidjson/istreamwrapper.h>

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
