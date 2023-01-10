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

#ifndef YSB_SOURCE_HPP
#define YSB_SOURCE_HPP

#include <fstream>
#include <vector>
#include <ff/ff.hpp>
#include "../util/event.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sent_tuples;

/** 
 *  @class Source_Functor
 *  
 *  @brief Define the logic of the Source
 */ 
class Source_Functor {
private:
    unsigned long **ads_arrays;
    unsigned int adsPerCampaign; // number of ads per campaign
    long generated_tuples; // tuples counter
    unsigned long current_time;
    unsigned int value;
    int rate; // stream generation rate
    unsigned long app_start_time; // application start time

    /** 
     *  @brief Add some active delay (busy-waiting function)
     *  
     *  @param waste_time wait time in microseconds
     */ 
    void active_delay(unsigned long waste_time) {
        auto start_time = current_time_usecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_usecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    // Constructor
    Source_Functor(int _rate,
                   unsigned long _app_start_time,
                   unsigned long **_ads_arrays,
                   unsigned int _adsPerCampaign):
            rate(_rate),
            app_start_time(_app_start_time),
            ads_arrays(_ads_arrays),
            adsPerCampaign(_adsPerCampaign),
            generated_tuples(0),
            value(0) {}

    /** 
     *  @brief Generation function of the input stream
     *  
     *  @param shipper Source_Shipper object used for generating inputs
     */ 
    void operator()(Source_Shipper<event_t> &shipper)
    {
        current_time = current_time_nsecs(); // get the current time
        // generation loop
        while (current_time - app_start_time <= app_run_time)
        {
            event_t event;
            event.user_id = 0; // not meaningful
            event.page_id = 0; // not meaningful
            event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
            event.ad_type = (value % 100000) % 5;
            event.event_type = (value % 100000) % 3;
            event.ip = 1; // not meaningful         
            value++;
            generated_tuples++;
            event.ts = current_time_nsecs();
            shipper.push(std::move(event)); // send the next tuple
            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0d / rate) * 1e9);
                active_delay(delay_nsec);
            }
            current_time = current_time_nsecs(); // get the new current time
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
    }

    ~Source_Functor() {}
};

#endif //YSB_SOURCE_HPP
