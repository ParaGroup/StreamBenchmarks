/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
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

#ifndef WORDCOUNT_SPLITTER_HPP
#define WORDCOUNT_SPLITTER_HPP

#include<ff/ff.hpp>
#include<string>
#include<vector>
#include<regex>
#include "../util/result.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Splitter_Functor class
class Splitter_Functor
{
private:
    size_t processed;
    size_t words;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t parallelism;
    size_t replica_id;

public:
    // Constructor
    Splitter_Functor(const unsigned long _app_start_time):
                     processed(0),
                     words(0),
                     app_start_time(_app_start_time),
                     current_time(_app_start_time) {}

    // operator() method
    void operator()(const string &t, Shipper<result_t> &shipper, RuntimeContext &rc)
    {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        istringstream line(t);
        string token;
        while (getline(line, token, ' ')) {
            result_t r;
            r.word = std::move(token);
            r.count = 1;
            shipper.push(std::move(r));
            words++;
        }
        processed++;
        // current_time = current_time_nsecs();
    }

    // Destructor
    ~Splitter_Functor()
    {
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
