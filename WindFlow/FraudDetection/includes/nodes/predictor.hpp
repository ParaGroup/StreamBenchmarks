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

#ifndef FRAUDDETECTION_PREDICTOR_HPP
#define FRAUDDETECTION_PREDICTOR_HPP

#include<ff/ff.hpp>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"
#include "../markov_model_prediction/model_based_predictor.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Predictor_Functor class
class Predictor_Functor
{
private:
    size_t processed;
    size_t outliers;
    Markov_Model_Predictor predictor;
    unordered_map<size_t, uint64_t> keys;
    unsigned long app_start_time;
    unsigned long current_time;
    size_t parallelism;
    size_t replica_id;

public:
    // Constructor
    Predictor_Functor(const unsigned long _app_start_time):
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
        Prediction prediction_object = predictor.execute(t.key, t.record, ",");
        processed++;
        if (prediction_object.is_outlier()) {
            t.score = prediction_object.get_score();
            outliers++;
            return true;
        }
        else {
            return false;
        }
    }

    // Destructor
    ~Predictor_Functor() {}
};

#endif //FRAUDDETECTION_PREDICTOR_HPP
