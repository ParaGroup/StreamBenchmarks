/**
 *  @file    prediction.hpp
 *  @author  Alessandra Fais
 *  @date    11/05/2019
 *
 *  @brief Definition of the structure of objects of type Prediction
 */

#ifndef FRAUDDETECTION_PREDICTION_HPP
#define FRAUDDETECTION_PREDICTION_HPP

#include <vector>
#include "windflow.hpp"

using namespace std;
using namespace wf;

/**
 *  @class Prediction
 *
 *  @brief Define the structure of objects of type Prediction
 */
class Prediction {
private:
    string entity_id;
    double_t score;
    vector<string> states;
    bool outlier;

public:

    /**
     *  @brief Constructor
     */
    Prediction(const string& _entity_id, double_t _score, const vector<string>& _states, bool outlier):
        entity_id(_entity_id), score(_score), states(_states), outlier(outlier) {}

    /// getter methods
    string get_entity() {
        return entity_id;
    }

    double_t get_score() {
        return score;
    }

    vector<string> get_states() {
        return states;
    }

    bool is_outlier() {
        return outlier;
    }

    /// setter methods
    void set_entity(const string& _entity_id) {
        entity_id = _entity_id;
    }

    void set_score(double_t _score) {
        score = _score;
    }

    void set_states(const vector<string>& _states) {
        states = _states;
    }

    void set_outlier(bool _outlier) {
        outlier = _outlier;
    }
};

#endif //FRAUDDETECTION_PREDICTION_HPP
