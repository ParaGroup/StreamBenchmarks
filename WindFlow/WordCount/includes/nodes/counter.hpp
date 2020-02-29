/** 
 *  @file    counter.hpp
 *  @author  Alessandra Fais
 *  @date    16/07/2019
 *
 *  @brief Node that counts the occurrences of each word and the number of bytes processed
 */

#ifndef WORDCOUNT_COUNTER_HPP
#define WORDCOUNT_COUNTER_HPP

#include <ff/ff.hpp>
#include <string>
#include <vector>
#include <regex>
#include "../util/tuple.hpp"
#include "../util/result.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/**
 *  @class Counter_Functor
 *
 *  @brief Define the logic of the Counter
 */
class Counter_Functor {
private:
    size_t processed;            // tuple counter (number of words)
    long bytes;                  // bytes counter

    // time variables
    unsigned long app_start_time;
    unsigned long current_time;

    // runtime information
    size_t parallelism;
    size_t replica_id;

public:

    /**
     *  @brief Constructor
     */
     Counter_Functor(const unsigned long _app_start_time):
            bytes(0L),
            processed(0),
            app_start_time(_app_start_time),
            current_time(_app_start_time) {}

     void operator()(const result_t& in, result_t& out, RuntimeContext& rc) {
         if (processed == 0) {
             parallelism = rc.getParallelism();
             replica_id = rc.getReplicaIndex();
         }
         //print_result("[Counter] Received tuple: ", in);
         out.key = in.key;
         out.id++;                          // number of occurrences of the string word
         out.ts = in.ts;
         bytes += (in.key).length();        // size of the string word (bytes)
         processed++;
         current_time = current_time_nsecs();
     }

     ~Counter_Functor() {
         if (processed != 0) {
             /*cout << "[Counter] replica " << replica_id + 1 << "/" << parallelism
                  << ", execution time: " << (current_time - app_start_time) / 1e09
                  << " s, processed: " << processed << " words (" << ((double)bytes / 1048576) << " MB)"
                  << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                  << " (words/s) " << ((double)bytes / 1048576) / ((double)(current_time - app_start_time) / 1e09)
                  << " (MB/s)" << endl;*/
         }
     }
};

#endif //WORDCOUNT_COUNTER_HPP
