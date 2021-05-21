/** 
 *  @file    constants.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/08/2019
 *  
 *  @brief Definition of useful constants
 */

#ifndef YSB_CONSTANTS_HPP
#define YSB_CONSTANTS_HPP

#include <string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology name
const string topology_name = "YSB";
const string source_name = "source";
const string filter_name = "filter";
const string joiner_name = "joiner";
const string winAgg_name = "winAggregate";
const string sink_name = "sink";

#endif //YSB_CONSTANTS_HPP
