/**
 *  @file    result.hpp
 *  @author  Alessandra Fais
 *  @date    14/07/2019
 *
 *  @brief Structure of a result tuple
 *
 *  This file defines the structure of the tuples generated by the splitter.
 *  The data type tuple_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setInfo() and
 *  getInfo() methods.
 */

#ifndef WORDCOUNT_RESULT_HPP
#define WORDCOUNT_RESULT_HPP

#include <windflow.hpp>

using namespace std;

struct result_t {
    string key;             // key word
    uint64_t count;            // id that indicates the current number of occurrences of the key word
    uint64_t ts;

    // default constructor
    result_t(): key(""), count(0) {}

    // constructor
    result_t(string &_key, uint64_t _count): key(_key), count(_count) {}
};

#endif //WORDCOUNT_RESULT_HPP
