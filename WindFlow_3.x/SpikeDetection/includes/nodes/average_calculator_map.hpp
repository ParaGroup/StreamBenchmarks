/** 
 *  @file    average_calculator_map.hpp
 *  @author  Alessandra Fais
 *  @date    18/06/2019
 *
 *  @brief Node that implements incremental mean value calculation
 */

#ifndef SPIKEDETECTION_AVERAGE_CALCULATOR_MAP_HPP
#define SPIKEDETECTION_AVERAGE_CALCULATOR_MAP_HPP

#include <ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/**
 *  @class Incremental_Average_Calculator
 *
 *  @brief Perform the incremental mean computation
 *
 *  The incremental calculation of the mean value is done over a window of items
 *  with the same key attribute. Since the query is incremental the processing is
 *  activated for each received input belonging to the window extent, and the
 *  corresponding window result (current sum value) is updated accordingly.
 */
class Incremental_Average_Calculator {
private:
    unordered_map<int, deque<double>> win_values; // for each device_id maintains a window containing the most recent values of the monitored property
    size_t win_size;                              // maximum number of values inside each window
    unordered_map<int, double> win_results;       // for each device_id maintains updated window results (sum of window elements)

public:

    /**
     *  @brief Constructor
     */
    Incremental_Average_Calculator(): win_size(_moving_avg_win_size) {}

    /**
     *  @brief Update windows and corresponding results
     *
     *  @param device_id key that identifies a device
     *  @param next_value value of the monitored property (temperature | humidity | light | voltage) contained in the current tuple
     *  @return mean value of the items currently in the window corresponding to device_id key
     */
    double compute(int device_id, double next_value) {
        if (win_values.find(device_id) != win_values.end()) { // device_id key is already present

            if (win_values.at(device_id).size() > win_size - 1) {            // control window size (number of values stored) for each device_id
                win_results.at(device_id) -= win_values.at(device_id).at(0); // decrement current window sum
                win_values.at(device_id).pop_front();                        // remove the older item in the window
            }

            win_values.at(device_id).push_back(next_value);     // add the last received value from this device
            win_results.at(device_id) += next_value;            // increment current window result (sum)

            //print_window(win_values.at(device_id));
            return win_results.at(device_id) / win_values.at(device_id).size(); // average value for the current window

        } else {    // device_id is not present
            win_values.insert(make_pair(device_id, deque<double>()));
            win_values.at(device_id).push_back(next_value);
            win_results.insert(make_pair(device_id, next_value));
            return next_value;
        }
    }

    ~Incremental_Average_Calculator() {}
};

/**
 *  @class Average_Calculator_Map
 *
 *  @brief Define the logic of the Average Calculator
 */
class Average_Calculator_Map_Functor {
private:
    size_t processed;       // tuples counter
    Incremental_Average_Calculator mean_calculator;
    unordered_map<size_t, uint64_t> keys;

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
    Average_Calculator_Map_Functor(const unsigned long _app_start_time):
            processed(0),
            app_start_time(_app_start_time),
            current_time(_app_start_time) {}

    /**
     *  @brief Update input tuples adding the current incremental average value
     *
     *  @param t input item to be updated and transmitted in output
     *  @param rc runtime context used to access to the parallelism degree and replica index
     */
    void operator()(tuple_t& t, RuntimeContext& rc) {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        //print_tuple("[AverageCalculator] received tuple: ", t);

        // set the incremental average field and send the tuple toward the next node
        t.incremental_average = mean_calculator.compute(t.key, t.property_value);
        processed++;

        //print_tuple("[AverageCalculator] sent tuple: ", t);

        // save the received keys (test keyed distribution)
        // if (keys.find(t.key) == keys.end())
        //     keys.insert(make_pair(t.key, t.id));
        // else
        //     (keys.find(t.key))->second = t.id;

        current_time = current_time_nsecs();
    }

    ~Average_Calculator_Map_Functor() {
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
