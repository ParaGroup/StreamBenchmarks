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

#ifndef FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP
#define FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP

#include "prediction.hpp"
#include "markov_model.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"
#include<list>
#include<map>

// Model_Based_Predictor class
class Model_Based_Predictor
{
public:
    virtual Prediction execute(size_t key, string record, string split_regex) = 0;
};

// Markov_Model_Predictor class
class Markov_Model_Predictor: public Model_Based_Predictor
{
private:
    Markov_Model markov_model; // Markov prediction model
    size_t mm_num_states; // number of states in the Markov model
    unordered_map<size_t, list<string>> records; // maintains a window of records relative to the same entity_id (key)
    size_t records_win_size; // number of records needed to close a window
    size_t state_position; // position of the state within a record string
    detection_algorithm alg; // algorithm used to detect the outliers
    double threshold; // value used to detect outliers (states sequences for which the score is higher than the threshold)
    int *max_state_prob_idx; // vector of maximum state probability index (valid only if algorithm is MISS_RATE)
    double *entropy; // vector of entropy values (valid only if algorithm is ENTROPY_REDUCTION)

    // get_local_metric method
    double get_local_metric(const vector<string> &states_sequence)
    {
        double params[2];
        params[0] = 0;
        params[1] = 0;
        // print states window
        //print_window(states_sequence);
        if (alg == MISS_PROBABILITY) {
            miss_probability(states_sequence, params);
        }
        else if (alg == MISS_RATE || alg == ENTROPY_REDUCTION) {
            miss_rate_OR_entropy_reduction(states_sequence, params);
        }
        return (params[0] / params[1]);
    }

    // miss_probability method
    void miss_probability(const vector<string> &states_sequence, double *params)
    {
        for (int i = 1; i < states_sequence.size(); i++) {
            size_t prev_state_idx = markov_model.get_index_of(states_sequence.at(i - 1));
            size_t cur_state_idx = markov_model.get_index_of(states_sequence.at(i));
            for (int j = 0; j < markov_model.get_num_states(); j++) {
                if (j != cur_state_idx) {
                    params[0] += markov_model.get_state_trans_prob()[prev_state_idx][j];
                }
            }
            params[1] += 1;
        }
    }

    // miss_rate_OR_entropy_reduction method
    void miss_rate_OR_entropy_reduction(const vector<string> &states_sequence, double *params)
    {
        for (int i = 1; i < states_sequence.size(); i++) {
            size_t prev_state_idx = markov_model.get_index_of(states_sequence.at(i - 1));
            size_t cur_state_idx = markov_model.get_index_of(states_sequence.at(i));
            params[0] += (cur_state_idx == max_state_prob_idx[prev_state_idx]) ? 0 : 1;
            params[1] += 1;
        }
    }

public:
    // Constructor
    Markov_Model_Predictor():
                           markov_model(_model_file),
                           records_win_size(_records_win_size),
                           state_position(_state_position),
                           alg(_alg),
                           threshold(_threshold)
    {
        mm_num_states = markov_model.get_num_states();
        if (alg == MISS_RATE) {
            // max probability state index
            max_state_prob_idx = new int[mm_num_states];
            for (int i = 0; i < mm_num_states; i++) {
                int max_prob_idx = -1;
                double max_prob = -1;
                for (int j = 0; j < mm_num_states; j++) {
                    if (markov_model.get_state_trans_prob()[i][j] > max_prob) {
                        max_prob = markov_model.get_state_trans_prob()[i][j];
                        max_prob_idx = j;
                    }
                }
                max_state_prob_idx[i] = max_prob_idx;
            }
        }
        else if (alg == ENTROPY_REDUCTION) {
            // entropy per source state
            entropy = new double[mm_num_states];
            for (int i = 0; i < mm_num_states; i++) {
                double ent = 0;
                for (int j = 0; j < mm_num_states; j++) {
                    ent += -markov_model.get_state_trans_prob()[i][j] * log(markov_model.get_state_trans_prob()[i][j]);
                }
                entropy[i] = ent; // for each state i evaluate the value of entropy
            }
        }
    }

    // execute method
    Prediction execute(size_t key, string record, string split_regex)
    {
        double score = 0;
        auto &this_records = records[key];
        this_records.push_back(record);
        if (this_records.size() > records_win_size) {
            this_records.pop_front();
        }
        vector<string> states_sequence(records_win_size);
        int i=0;
        if (this_records.size() == records_win_size) {
            for (const string& r : this_records) {  // read the sequence of states from the records in the window
                if (state_position == 0)
                    states_sequence[i] = r.substr(0, r.find(split_regex));
                else if (state_position == 1)
                    states_sequence[i] = r.substr(r.find(split_regex) + 1, r.size());
                i++;
            }
            score = get_local_metric(states_sequence);
        }
        Prediction prediction(key, score, states_sequence, (score > threshold));
        return prediction;
    }

    // get_num_states method
    size_t get_num_states()
    {
        return mm_num_states;
    }

    // get_max_state_prob_idx method
    int *get_max_state_prob_idx()
    {
        return max_state_prob_idx;
    }

    // get_entropy method
    double *get_entropy()
    {
        return entropy;
    }

    // get_model_alg method
    int get_model_alg()
    {
        return alg;
    }

    // Destructor
    ~Markov_Model_Predictor() {}
};

#endif //FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP
