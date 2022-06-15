/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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
