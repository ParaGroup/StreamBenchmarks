/** 
 *  @file    result.hpp
 *  @author  Alessandra Fais
 *  @date    16/05/2019
 *  
 *  @brief Structure of a tuple result of the FlatMap
 *  
 *  This file defines the structure of the tuples generated by the predictor.
 */ 

#ifndef FRAUDDETECTION_RESULT_HPP
#define FRAUDDETECTION_RESULT_HPP

#include <windflow.hpp>

using namespace std;

// struct result_t
struct result_t
{
    string entity_id; // identifies the customer
    double_t score; // indicates the chances of fraudolent activity associated with the transaction sequence
    vector<string> states; // representation of the transaction sequence
    size_t key; // key attribute
    uint64_t ts;

    // Constructor I
    result_t(): entity_id(""), score(0.0), key(0) {}

    // Constructor II
    result_t(const string &_entity_id,
    	     double_t _score,
    	     const vector<string> &_states,
    	     size_t _key):
             entity_id(_entity_id),
             score(_score),
             states(_states),
             key(_key) {}
};

#endif //FRAUDDETECTION_RESULT_HPP
