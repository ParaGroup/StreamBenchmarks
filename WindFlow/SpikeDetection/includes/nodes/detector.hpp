/** 
 *  @file    detector.hpp
 *  @author  Alessandra Fais
 *  @date    05/06/2019
 *
 *  @brief Node that implements spike detection
 */

#ifndef SPIKEDETECTION_DETECTOR_HPP
#define SPIKEDETECTION_DETECTOR_HPP

#include <ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/**
 *  @class Detector_Functor
 *
 *  @brief Define the logic of the Spike Detector
 */
class Detector_Functor {
private:
    size_t processed;       // tuples counter
    size_t outliers;        // spikes counter

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
    Detector_Functor(const unsigned long _app_start_time):
            processed(0),
            outliers(0),
            app_start_time(_app_start_time),
            current_time(_app_start_time) {}

    /**
     *  @brief Detect spikes
     *
     *  @param t input tuple
     *  @return true if a spike has been found, false otherwise
     */
    bool operator()(tuple_t& t, RuntimeContext& rc) {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        //print_tuple("[Detector] Received tuple: ", t);
        processed++;
        current_time = current_time_nsecs();

        if (abs(t.property_value - t.incremental_average) > (_threshold * t.incremental_average)) {
            outliers++;
            return true;
        } else
            return false;
    }

    ~Detector_Functor() {
        /*if (processed != 0) {
            cout << "[Detector] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1e09
                 << " s, processed: " << processed
                 << ", outliers: " << outliers
                 << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                 << endl;
        }*/
    }
};

#endif //SPIKEDETECTION_DETECTOR_HPP
