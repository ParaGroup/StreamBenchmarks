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

#ifndef YSB_RESULT_HPP
#define YSB_RESULT_HPP

#include <windflow.hpp>

using namespace std;

// win_result struct
struct result_t
{
    unsigned long wid; // id
    unsigned long cmp_id; // campaign id
    unsigned long lastUpdate; // MAX(TS)
    unsigned long count; // COUNT(*)

    // Constructor I
    result_t():
             wid(0),
             cmp_id(0),
             lastUpdate(0),
             count(0) {}

    // Constructor II
    result_t(unsigned long _cmp_id,
             uint64_t _wid):
             lastUpdate(0),
             count(0),
             cmp_id(_cmp_id),
             wid(_wid) {}
};

#endif //YSB_RESULT_HPP
