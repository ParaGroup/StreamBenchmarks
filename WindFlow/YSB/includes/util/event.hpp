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

#ifndef YSB_EVENT_HPP
#define YSB_EVENT_HPP

#include <windflow.hpp>

using namespace std;

// event_t struct
struct event_t
{
    unsigned long user_id; // user id
    unsigned long page_id; // page id
    unsigned long ad_id; // advertisement id
    unsigned int ad_type; // advertisement type (0, 1, 2, 3, 4) => ("banner", "modal", "sponsored-search", "mail", "mobile")
    unsigned int event_type; // event type (0, 1, 2) => ("view", "click", "purchase")
    unsigned int ip; // ip address
    uint64_t ts;
    //char padding[28]; // padding
};

#endif //YSB_EVENT_HPP
