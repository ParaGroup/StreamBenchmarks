/** 
 *  @file    markov_model.hpp
 *  @author  Alessandra Fais
 *  @date    11/05/2019
 *
 *  @brief Definition of the Markov model used by the predictor
 */

#ifndef FRAUDDETECTION_MARKOV_MODEL_HPP
#define FRAUDDETECTION_MARKOV_MODEL_HPP

#include <iostream>
#include <sstream>
#include "windflow.hpp"

using namespace std;
using namespace wf;

/**
 *  @class Markov_Model
 *
 *  @brief Define the structure of objects of type Markov_Model
 */
class Markov_Model {
private:
    vector<string> states;
    size_t num_states;
    double** state_trans_prob;

public:

    /**
     *  @brief Constructor
     */
    Markov_Model(const string& model_file) {
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

    /// getter methods
    vector<string> get_states() {
        return states;
    }

    size_t get_num_states() {
        return num_states;
    }

    double** get_state_trans_prob() {
        return state_trans_prob;
    }

    size_t get_index_of(const string& state) {
        size_t index = -1;
        for (size_t i = 0; i < num_states; i++) {
            if (states.at(i) == state) index = i;
        }
        return index;
    }
};

#endif //FRAUDDETECTION_MARKOV_MODEL_HPP
