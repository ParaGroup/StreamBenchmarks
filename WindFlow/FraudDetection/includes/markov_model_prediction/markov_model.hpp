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

#ifndef FRAUDDETECTION_MARKOV_MODEL_HPP
#define FRAUDDETECTION_MARKOV_MODEL_HPP

#include<iostream>
#include<sstream>
#include "windflow.hpp"

using namespace std;
using namespace wf;

// Markov_Model class
class Markov_Model
{
private:
    vector<string> states;
    size_t num_states;
    double **state_trans_prob;

public:
    // Constructor
    Markov_Model(const string &model_file)
    {
        ifstream file(model_file);
        if (file.is_open()) {
            //cout << "Reading " << model_file << endl;
            string line;
            int line_count = 0;
            int row = 0;
            while (getline(file, line)) {
                //cout << "line " << line_count << ": " << line << endl;
                if (line_count == 0) {
                    // read the states of the Markov model from the file
                    istringstream iss(line);
                    string token;
                    while (getline(iss, token, ',')) {
                        states.insert(states.end(), token);
                    }
                    num_states = states.size();

                    // initialize the one step state transition probability matrix
                    state_trans_prob = new double*[num_states];
                    for (int i = 0; i < num_states; i++) {
                        state_trans_prob[i] = new double[num_states]; // build rows
                    }
                } else {
                    // read the states transition probabilities
                    istringstream iss(line);
                    string token;
                    vector<string> prob_row;
                    while (getline(iss, token, ',')) {
                        prob_row.insert(prob_row.end(), token);
                    }
                    if (prob_row.size() != num_states)
                        __throw_invalid_argument("[MarkovModel] Error defining the state transition probability matrix");

                    // fill the one step state transition probability matrix
                    for (int p = 0; p < num_states; p++) {
                        // fill matrix row
                        state_trans_prob[row][p] = atof((prob_row.at(p)).c_str());
                    }
                    row++;
                }
                line_count++;
            }
            file.close();
        }
    }

    // get_states method
    vector<string> get_states()
    {
        return states;
    }

    // get_num_states method
    size_t get_num_states()
    {
        return num_states;
    }

    // get_state_trans_prob method
    double **get_state_trans_prob()
    {
        return state_trans_prob;
    }

    // get_index_of method
    size_t get_index_of(const string &state)
    {
        size_t index = -1;
        for (size_t i = 0; i < num_states; i++) {
            if (states.at(i) == state) index = i;
        }
        return index;
    }
};

#endif //FRAUDDETECTION_MARKOV_MODEL_HPP
