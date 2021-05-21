/** 
 *  @file    source.hpp
 *  @author  Alessandra Fais
 *  @date    18/06/2019
 *
 *  @brief Source node that generates the input stream
 *
 *  The source node generates the stream by reading the tuples from memory.
 */

#ifndef WORDCOUNT_SOURCE_HPP
#define WORDCOUNT_SOURCE_HPP

#include <fstream>
#include <vector>
#include <ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> total_lines;
extern atomic<long> total_bytes;

/**
 *  @class Source_Functor
 *
 *  @brief Define the logic of the Source
 */
class Source_Functor {
private:
    vector<tuple_t> dataset;        // contains all the tuples
    int rate;                       // stream generation rate
    size_t next_tuple_idx;          // index of the next tuple to be sent
    int generations;                // counts the times the file is generated
    long generated_tuples;          // tuples counter
    long generated_bytes;           // bytes counter

    // time variables
    unsigned long app_start_time;   // application start time
    unsigned long current_time;
    unsigned long interval;

    /**
     *  @brief Add some active delay (busy-waiting function)
     *
     *  @param waste_time wait time in nanoseconds
     */
    void active_delay(unsigned long waste_time) {
        auto start_time = current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    /**
     *  @brief Constructor.
     *
     *  @param _dataset all the tuples that will compose the stream
     *  @param _rate stream generation rate
     *  @param _app_start_time application starting time
     */
    Source_Functor(const vector<tuple_t>& _dataset,
                   const int _rate,
                   const unsigned long _app_start_time):
            rate(_rate),
            app_start_time(_app_start_time),
            current_time(_app_start_time),
            next_tuple_idx(0),
            generations(0),
            generated_tuples(0),
            generated_bytes(0)
    {
        dataset = _dataset;
        interval = 1000000L; // 1 second (microseconds)
    }

    /** 
     *  @brief Generation function of the input stream
     *  
     *  @param shipper Source_Shipper object used for generating inputs
     */ 
	void operator()(Source_Shipper<tuple_t> &shipper)
	{
    	current_time = current_time_nsecs(); // get the current time
    	// generation loop
    	while (current_time - app_start_time <= app_run_time)
    	{
    		if (next_tuple_idx == 0) {
    			generations++;
    		}
    		tuple_t t(dataset.at(next_tuple_idx));
    		t.ts = current_time_nsecs();
    		generated_tuples++;
    		generated_bytes += (t.text_line).size();
    		shipper.push(std::move(t)); // send the next tuple
    		next_tuple_idx = (next_tuple_idx + 1) % dataset.size();   // index of the next tuple to be sent (if any)
	        if (rate != 0) { // active waiting to respect the generation rate
	            long delay_nsec = (long) ((1.0d / rate) * 1e9);
	            active_delay(delay_nsec);
	        }
	        current_time = current_time_nsecs(); // get the new current time
    	}
        total_lines.fetch_add(generated_tuples);
        total_bytes.fetch_add(generated_bytes);
	}
};

#endif //WORDCOUNT_SOURCE_HPP
