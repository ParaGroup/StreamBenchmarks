/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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

#ifndef TAXI_SOURCE_HPP
#define TAXI_SOURCE_HPP

#include<zlib.h>
#include<ctime>
#include<atomic>
#include<string>
#include<vector>
#include<random>
#include<chrono>
#include<thread>
#include<memory>
#include<cstdio>
#include<iostream>
#include<fstream>
#include<sstream>
#include<stdexcept>
#include<windflow.hpp>
#include"taxi_ride.hpp"

using namespace wf;

extern std::atomic<long> sent_tuples;

// Source_Functor class
class TaxiSource_Functor
{
private:
    int generated_tuples; // number of generated tuples
    int batch_size; // size of the batches used by the source
    int app_runtime_sec; // maximum running time
    unsigned long app_start_time_nsec; // initial application starting time
    uint64_t next_ts; // next timestamp to be used
    std::string dataFilePath; // pathname where to find the dataset

public:
    // Constructor
    TaxiSource_Functor(const std::string &_dataFilePath,
                       const int _batch_size,
                       const unsigned long _app_runtime_sec,
                       const unsigned long _app_start_time_nsec):
                       generated_tuples(0),
                       batch_size(_batch_size),
                       app_runtime_sec(_app_runtime_sec), 
                       app_start_time_nsec(_app_start_time_nsec),
                       next_ts(0),
                       dataFilePath(_dataFilePath) {}

    // operator() method
    void operator()(Source_Shipper<TaxiRide> &shipper)
    {  
        char buffer[2048];
        gzFile file = gzopen(dataFilePath.c_str(), "rb");
        if (!file) {
            std::cerr << "Error opening the dataset compressed file" << std::endl;
            exit(EXIT_FAILURE);
        }
        unsigned long current_time = current_time_nsecs(); // get the current time
        while (current_time - app_start_time_nsec <= app_runtime_sec * 1e9) // generation loop
        {
            if (gzgets(file, buffer, sizeof(buffer)) != Z_NULL) {
                std::string line(buffer);
                if (!line.empty()) {
                    try {
                        TaxiRide ride = TaxiRide::fromString(line);
                        shipper.pushWithTimestamp(std::move(ride), next_ts); // send the next tuple
                        shipper.setNextWatermark(next_ts);
                        next_ts += 5000; // fixed offset of 5000 usec between two events
                        generated_tuples++;
                    }
                    catch (const std::runtime_error &e) {
                        std::cerr << "Invalid record: " << e.what() << std::endl;
                    }
                }
            }
            else {
                gzclose(file);
                gzFile file = gzopen(dataFilePath.c_str(), "rb");
                if (!file) {
                    std::cerr << "Error opening the dataset compressed file" << std::endl;
                    exit(EXIT_FAILURE);
                }                
            }
            if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            else {
                current_time = current_time_nsecs(); // get the new current time
            }
        }
        sent_tuples.fetch_add(generated_tuples); // save the number of generated tuples
    }

    // Destructor
    ~TaxiSource_Functor() {}
};

#endif
