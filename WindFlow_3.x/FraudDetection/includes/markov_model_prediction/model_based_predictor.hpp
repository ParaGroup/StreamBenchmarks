/** 
 *  @file    model_based_predictor.hpp
 *  @author  Alessandra Fais
 *  @date    16/05/2019
 *
 *  @brief Definition of the model based predictor
 */

#ifndef FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP
#define FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP

#include "prediction.hpp"
#include "markov_model.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"
#include <list>
#include <map>

/**
 *  @class Model_Based_Predictor
 *
 *  @brief Base class for all model based predictors
 */
class Model_Based_Predictor {
public:
    virtual Prediction execute(string entity_id, string record, string split_regex) = 0;
};

/**
 *  @class Markov_Model_Predictor
 *
 *  @brief Predictor based on Markov model
 *
 *  The class derives from the base class Model_Based_Predictor.
 *  The model parameters can be set in util/constants.hpp.
 */
class Markov_Model_Predictor: public Model_Based_Predictor {
private:
    Markov_Model markov_model;                                  // Markov prediction model
    size_t mm_num_states;                                       // number of states in the Markov model
    unordered_map<string, list<string>> records;               // maintains a window of records relative to the same entity_id
    size_t records_win_size;                                    // number of records needed to close a window
    size_t state_position;                                      // position of the state within a record string
    detection_algorithm alg;                                    // algorithm used to detect the outliers
    double threshold;                                           // value used to detect outliers (states sequences for which the score is higher than the threshold)
    int* max_state_prob_idx;                                    // vector of maximum state probability index (valid only if algorithm is MISS_RATE)
    double* entropy;                                            // vector of entropy values (valid only if algorithm is ENTROPY_REDUCTION)

    /**
     *  @brief Compute the score relative to the current window of states
     *
     *  The way the score is computed depends on the detection algorithm.
     *  @param states_sequence the window of states to be processed
     *  @return the score value
     */
    double get_local_metric(const vector<string>& states_sequence) {
        double params[2];
        params[0] = 0;
        params[1] = 0;

        // print states window
        //print_window(states_sequence);

        if (alg == MISS_PROBABILITY)
            miss_probability(states_sequence, params);
        else if (alg == MISS_RATE || alg == ENTROPY_REDUCTION)
            miss_rate_OR_entropy_reduction(states_sequence, params);

        return (params[0] / params[1]);
    }

    /**
     *  @brief Compute the score when the detection algorithm is MissProbability
     *
     *  @param states_sequence the window of states to be processed
     *  @param params to be filled with the result of the computation
     */
    void miss_probability(const vector<string>& states_sequence, double* params) {
        for (int i = 1; i < states_sequence.size(); i++) {
            size_t prev_state_idx = markov_model.get_index_of(states_sequence.at(i - 1));
            size_t cur_state_idx = markov_model.get_index_of(states_sequence.at(i));

            // print indexes used to access the one step probability matrix
            //print_prob_indexes(states_sequence.at(i - 1), states_sequence.at(i), prev_state_idx, cur_state_idx);

            for (int j = 0; j < markov_model.get_num_states(); j++) {
                if (j != cur_state_idx) {
                    params[0] += markov_model.get_state_trans_prob()[prev_state_idx][j];
                }
            }
            params[1] += 1;
        }
    }

    /**
     *  @brief Compute the score when the detection algorithm is MissRate or EntropyReduction
     *
     *  @param states_sequence the window of states to be processed
     *  @param params to be filled with the result of the computation
     */
    void miss_rate_OR_entropy_reduction(const vector<string>& states_sequence, double* params) {
        for (int i = 1; i < states_sequence.size(); i++) {
            size_t prev_state_idx = markov_model.get_index_of(states_sequence.at(i - 1));
            size_t cur_state_idx = markov_model.get_index_of(states_sequence.at(i));

            // print indexes used to access the one step probability matrix
            //print_prob_indexes(states_sequence.at(i - 1), states_sequence.at(i), prev_state_idx, cur_state_idx);

            params[0] += (cur_state_idx == max_state_prob_idx[prev_state_idx]) ? 0 : 1;
            params[1] += 1;
        }
    }

public:

    /**
     *  @brief Constructor
     */
    Markov_Model_Predictor():
        markov_model(_model_file),
        records_win_size(_records_win_size),
        state_position(_state_position),
        alg(_alg),
        threshold(_threshold)
    {
        // print model state
        //print_model_parameters(_model_file, records_win_size, state_position, alg, threshold);

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
                // save for each state i the state j s.t. the probability H[i, j] of going from i to j at the
                // next step is the higher between all probabilities of moving from i (find the maximum value
                // contained in row i of the one step transition probability matrix)
                max_state_prob_idx[i] = max_prob_idx;
            }
        } else if (alg == ENTROPY_REDUCTION) {
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

    /**
     *  @brief Apply the detection algorithm and find the outliers
     *
     *  Produce an object of type Prediction for each window of states that can be closed and
     *  associated to an entity_id. This object also contains the score and a flag that mark it
     *  as outlier or not.
     *  @param entity_id the entity that identifies the customer
     *  @param record contains the transaction id and the transaction type (state in the Markov model)
     *  @param split_regex regular expression used to estract the state from each record
     *  @return an object of type Prediction
     */
    Prediction execute(string entity_id, string record, string split_regex) {
        

        double score = 0;
        auto &this_records = records[entity_id];
        this_records.push_back(record);

        //if (records.find(entity_id) == records.end())
        //    records.insert(make_pair(entity_id, list<string>()));

        //records.at(entity_id).push_back(record);

        // control the window size (number of records stored) for each entity_id
        if (this_records.size() > records_win_size)
            this_records.pop_front();
        // if the window can be closed (it's size is equal to records_win_size) evaluate the score related
        // to the sequence of states extracted from the records in the window; compute the score using the
        // specified detection algorithm (the metric): if the score exceeds the threshold an outlier has been
        // detected and the sequence of states (bank transaction types) identifies a fraudolent activity
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
        /*// create a string representation of the sequence of states
        string states;
        for (const string& s : states_sequence)
            states += s + " ";*/

        Prediction prediction(entity_id, score, states_sequence, (score > threshold));
        //if (score > threshold)  // outlier
        //    print_fraudolent_sequence(states_sequence, score, threshold);
        
        return prediction;
    }

    /// getter methods
    size_t get_num_states() {
        return mm_num_states;
    }

    int* get_max_state_prob_idx() {
        return max_state_prob_idx;
    }

    double* get_entropy() {
        return entropy;
    }

    int get_model_alg() {
        return alg;
    }

    ~Markov_Model_Predictor() {}
};

#endif //FRAUDDETECTION_MODEL_BASED_PREDICTOR_HPP
