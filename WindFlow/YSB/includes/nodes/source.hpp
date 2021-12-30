/** 
 *  @file    source.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/08/2019
 *  
 *  @brief Source node that generates the input stream
 *  
 *  The source node generates the stream by reading the tuples from memory.
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
     *  @brief Send tuples in a item-by-item fashion
     *  
     *  @param t reference to the event structure
     *  @return true if the stream is not ended, false if the EOS has been reached
     */ 
    bool operator()(event_t &event) {
        if (rate != 0) {
            long delay_usec = (long) ((1.0d / rate) * 1e06);
            active_delay(delay_usec);
        }
        current_time = current_time_usecs();
        // fill the event's fields
        event.ts = current_time_usecs() - app_start_time;
        event.user_id = 0; // not meaningful
        event.page_id = 0; // not meaningful
        event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
        event.ad_type = (value % 100000) % 5;
        event.event_type = (value % 100000) % 3;
        event.ip = 1; // not meaningful
        value++;
        generated_tuples++;
        double elapsed_time = (current_time - app_start_time);
        if (elapsed_time >= app_run_time) {
            sent_tuples.fetch_add(generated_tuples);
            return false;
        }
        else {
            return true;
        }
    }

    ~Source_Functor() {}
};

#endif //YSB_SOURCE_HPP
