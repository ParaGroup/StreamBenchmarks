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

#ifndef YSB_JOINER_HPP
#define YSB_JOINER_HPP

#include <ff/ff.hpp>
#include <string>
#include <vector>
#include <regex>
#include <unordered_map>
#include "../util/event.hpp"
#include "../util/joined_event.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"
#include "../util/campaign_generator.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/** 
 *  @class Joiner_Functor
 *  
 *  @brief Define the logic of the Joiner
 */ 
class Joiner_Functor {
private:
    unordered_map<unsigned long, unsigned int> &map; // hashmap
    campaign_record *relational_table; // relational table

public:
    // Constructor
    Joiner_Functor(unordered_map<unsigned long, unsigned int> &_map,
                   campaign_record *_relational_table):
            map(_map),
            relational_table(_relational_table) {}

    void operator()(const event_t &event, Shipper<joined_event_t> &shipper) {
        // check inside the hashmap
        auto it = map.find(event.ad_id);
        if (it == map.end()) {
            return;
        }
        else {
            campaign_record record = relational_table[(*it).second];
            joined_event_t out(record.cmp_id, 0);
            out.ts = event.ts;
            out.ad_id = event.ad_id;
            out.relational_ad_id = record.ad_id;
            out.cmp_id = record.cmp_id;
            shipper.push(std::move(out));
        }
    }
};

#endif //YSB_JOINER_HPP
