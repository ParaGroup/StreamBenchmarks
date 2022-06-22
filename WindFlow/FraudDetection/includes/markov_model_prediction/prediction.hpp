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

#ifndef FRAUDDETECTION_PREDICTION_HPP
#define FRAUDDETECTION_PREDICTION_HPP

#include<vector>
#include "windflow.hpp"

using namespace std;
using namespace wf;

// Prediction class
class Prediction
{
private:
    size_t key;
    string entity_id;
    double_t score;
    vector<string> states;
    bool outlier;

public:
    // Constructor
    Prediction(size_t &_key, double_t _score, const vector<string> &_states, bool outlier):
               key(_key), score(_score), states(_states), outlier(outlier) {}

    // get_key method
    size_t get_key()
    {
        return key;
    }

    // get_score method
    double_t get_score()
    {
        return score;
    }

    // get_states method
    vector<string> get_states()
    {
        return states;
    }

    // is_outlier method
    bool is_outlier()
    {
        return outlier;
    }

    // set_key method
    void set_key(size_t _key)
    {
        key = _key;
    }

    // set_entity method
    void set_entity(const string &_entity_id)
    {
        entity_id = _entity_id;
    }

    // set_score method
    void set_score(double_t _score)
    {
        score = _score;
    }

    // set_states method
    void set_states(const vector<string> &_states)
    {
        states = _states;
    }

    // set_outlier method
    void set_outlier(bool _outlier)
    {
        outlier = _outlier;
    }
};

#endif //FRAUDDETECTION_PREDICTION_HPP
