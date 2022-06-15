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

#ifndef SPIKEDETECTION_AVERAGE_CALCULATOR_MAP_HPP
#define SPIKEDETECTION_AVERAGE_CALCULATOR_MAP_HPP

#include<ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Incremental_Average_Calculator class
class Incremental_Average_Calculator
{
private:
    unordered_map<int, deque<double>> win_values;
    size_t win_size;
    unordered_map<int, double> win_results;

public:
    // Constructor
    Incremental_Average_Calculator():
                                   win_size(_moving_avg_win_size) {}

    // compute method
    double compute(int device_id, double next_value)
    {
        if (win_values.find(device_id) != win_values.end()) {
            if (win_values.at(device_id).size() > win_size - 1) {
                win_results.at(device_id) -= win_values.at(device_id).at(0);
                win_values.at(device_id).pop_front();
            }
            win_values.at(device_id).push_back(next_value);
            win_results.at(device_id) += next_value;
            return win_results.at(device_id) / win_values.at(device_id).size();
        }
        else { // device_id is not present
            win_values.insert(make_pair(device_id, deque<double>()));
            win_values.at(device_id).push_back(next_value);
            win_results.insert(make_pair(device_id, next_value));
            return next_value;
        }
    }

    // Destructor
    ~Incremental_Average_Calculator() {}
};

// Average_Calculator_Map_Functor class
class Average_Calculator_Map_Functor
{
private:
    size_t processed;
    Incremental_Average_Calculator mean_calculator;
    unordered_map<size_t, uint64_t> keys;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t parallelism;
    size_t replica_id;

public:
    // Constructor
    Average_Calculator_Map_Functor(const unsigned long _app_start_time):
                                   processed(0),
                                   app_start_time(_app_start_time),
                                   current_time(_app_start_time) {}

    // operator() method
    void operator()(tuple_t &t, RuntimeContext &rc)
    {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        t.incremental_average = mean_calculator.compute(t.key, t.property_value); // set the incremental average field and send the tuple toward the next node
        processed++;
        //print_tuple("[AverageCalculator] sent tuple: ", t);
        // save the received keys (test keyed distribution)
        // if (keys.find(t.key) == keys.end())
        //     keys.insert(make_pair(t.key, t.id));
        // else
        //     (keys.find(t.key))->second = t.id;
        // current_time = current_time_nsecs();
    }

    // Destructor
    ~Average_Calculator_Map_Functor()
    {
        //if (processed != 0) {
            /*cout << "[AverageCalculator] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1e09
                 << " s, processed: " << processed
                 << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                 << ", #keys: " << keys.size()
                 << endl;*/
            // print received keys and number of occurrences
            /*     << ", keys: "
                 << endl;
            for (auto k : keys) {
               cout << "key: " << k.first << " id: " << k.second << endl;
            }*/
        //}
    }
};

#endif //SPIKEDETECTION_AVERAGE_CALCULATOR_MAP_HPP
