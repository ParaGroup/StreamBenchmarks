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

#ifndef SPIKEDETECTION_DETECTOR_HPP
#define SPIKEDETECTION_DETECTOR_HPP

#include<ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Detector_Functor class
class Detector_Functor
{
private:
    size_t processed;
    size_t outliers;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t parallelism;
    size_t replica_id;

public:
    // Constructor
    Detector_Functor(const unsigned long _app_start_time):
                     processed(0),
                     outliers(0),
                     app_start_time(_app_start_time),
                     current_time(_app_start_time) {}

    // operator() method
    bool operator()(tuple_t &t, RuntimeContext &rc)
    {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        processed++;
        // current_time = current_time_nsecs();
        if (abs(t.property_value - t.incremental_average) > (_threshold * t.incremental_average)) {
            outliers++;
            return true;
        }
        else {
            return false;
        }
    }

    // Destructor
    ~Detector_Functor()
    {
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
