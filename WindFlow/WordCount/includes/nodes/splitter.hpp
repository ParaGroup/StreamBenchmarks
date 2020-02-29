/** 
 *  @file    splitter.hpp
 *  @author  Alessandra Fais
 *  @date    16/07/2019
 *
 *  @brief Node that splits each received text line into single words
 */

#ifndef WORDCOUNT_SPLITTER_HPP
#define WORDCOUNT_SPLITTER_HPP

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
 *  @class Splitter_Functor
 *
 *  @brief Define the logic of the Splitter
 */
class Splitter_Functor {
private:
    size_t processed;       // tuples counter (number of text lines processed)
    size_t words;           // words counter (number of words produced as output of the line splitting process)

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
    Splitter_Functor(const unsigned long _app_start_time):
            processed(0),
            words(0),
            app_start_time(_app_start_time),
            current_time(_app_start_time) {}

    void operator()(const tuple_t& t, Shipper<result_t>& shipper, RuntimeContext& rc) {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        //print_tuple("[Splitter] Received tuple: ", t);
#if 1
        istringstream line(t.text_line);
        string token;
        while (getline(line, token, ' ')) {
            result_t *r = new result_t();
            r->key = token;
            r->ts = t.ts;
            shipper.push(r);
            words++;
        }
#else
        istringstream ss(t.text_line);
        do { 
            // read a word
            string word;
            ss >> word;
            result_t *r = new result_t();
            r->key = word;
            r->ts = t.ts;
            shipper.push(r);
            words++;
        }
        while (ss);
#endif
        processed++;
        current_time = current_time_nsecs();
    }

     ~Splitter_Functor() {
         if (processed != 0) {
             /*cout << "[Splitter] replica " << replica_id + 1 << "/" << parallelism
                  << ", execution time: " << (current_time - app_start_time) / 1e09
                  << " s, processed: " << processed << " lines (" << words << " words)"
                  << ", bandwidth: " << words / ((current_time - app_start_time) / 1e09)
                  << " words/s" << endl;*/
         }
     }
};

#endif //WORDCOUNT_SPLITTER_HPP
