/**
 *  @file    constants.hpp
 *  @author  Alessandra Fais
 *  @date    07/06/2019
 *
 *  @brief Definition of useful constants
 */

#ifndef WORDCOUNT_CONSTANTS_HPP
#define WORDCOUNT_CONSTANTS_HPP

#include <string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology name
const string topology_name = "WordCount";
const string source_name = "source";
const string splitter_name = "splitter";
const string counter_name = "counter";
const string sink_name = "sink";


const string _input_file = "../../Datasets/WC/book.dat";           // path of the dataset to be used

#endif //WORDCOUNT_CONSTANTS_HPP
