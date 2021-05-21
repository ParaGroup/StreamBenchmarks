#pragma once

#include "configuration.hpp"
#include <iostream>

namespace util {

template <typename Builder>
Builder setup(const std::string &name, const util::Configuration &configuration, Builder builder) {
    int parallelism_hint = configuration.get_tree()[name.c_str()].GetInt();
    auto chaining = configuration.get_tree()["chaining"].GetBool();

    std::cout << "NODE: " << name << " ("<< parallelism_hint << ")\n";

	    return builder
	        .withName(name)
	        .withParallelism(parallelism_hint);
}

}
