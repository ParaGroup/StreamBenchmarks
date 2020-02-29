/** 
 *  @file    filter.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/08/2019
 *  
 *  @brief Node that filters input tuples based on event type
 */ 

#ifndef YSB_FILTER_HPP
#define YSB_FILTER_HPP

#include <ff/ff.hpp>
#include <string>
#include <vector>
#include <regex>
#include "../util/event.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/** 
 *  @class Filter_Functor
 *  
 *  @brief Define the logic of the Filter
 */ 
class Filter_Functor {
private:
    unsigned int event_type; // forward only tuples with event_type

public:
    // Constructor
    Filter_Functor(): event_type(0) {}

    bool operator()(event_t &event) {
        if (event.event_type == event_type)
            return true;
        else
            return false;
    }
};

#endif //YSB_FILTER_HPP
