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

#include<ctime>
#include<string>
#include<vector>
#include<sstream>
#include<iomanip>
#include<iostream>
#include<windflow.hpp>
#include<persistent/windflow_rocksdb.hpp>
#include<taxi_ride.hpp>
#include<taxi_result.hpp>
#include<taxi_source.hpp>
#include<taxi_sink.hpp>
#include<window_q1.hpp>
#include<window_q2.hpp>
#include<window_q3.hpp>

#define MEM_TABLE_SIZE_MB 256
#define NUM_MEM_TABLE 2
#define FRAGMENT_SIZE 64

using namespace wf;
using namespace std;
using namespace chrono;

std::atomic<long> sent_tuples; // total number of tuples sent by all the sources

// Main
int main(int argc, char* argv[])
{
    /// parse arguments from command line
    int option = 0;
    std::string dataset_path = "/home/mencagli/StreamBenchmarks/Datasets/Taxi/nycTaxiData_extended.gz";
    size_t parallelism = 0;
    sent_tuples = 0;
    int query_id = 0;
    size_t batch_size = 0;
    bool isPersisent = false;
    size_t cache_size = 0;
    const int app_runtime_sec = 1200;
    const int q1_win_len_sec = 300;
    const int q1_slide_len_sec = 60;
    const int q2_win_len_sec = 300;
    const int q2_slide_len_sec = 10;
    const int q3_win_len_sec = 300;
    const int q3_slide_len_sec = 10;
    if (argc >= 7 && argc <= 10) {
        while ((option = getopt(argc, argv, "q:p:b:c:r")) != -1) {
            switch (option) {
                case 'q': {
                    query_id = atoi(optarg);
                    if (query_id < 1 || query_id > 3) {
                        std::cerr << "Query identifier not valid (it should be [1..3])" << std::endl;
                    }
                    break;
                }
                case 'p': {
                    parallelism = atoi(optarg);
                    break;
                }
                case 'b': {
                    batch_size = atoi(optarg);
                    break;
                }
                case 'c': {
                    cache_size = atoi(optarg);
                    break;
                }
                case 'r': {
                    isPersisent = true;
                    break;
                }
                default: {
                    std::cerr << "Usage: " << argv[0] << " -q <query_id> -p <parallelism> -b <batch_size> [-c <cache_size>] [-r]" << std::endl;
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else if (argc == 2) {
        while ((option = getopt(argc, argv, "h")) != -1) {
            switch (option) {
                case 'h': {
                    std::cerr << "Usage: " << argv[0] << " -q <query_id> -p <parallelism> -b <batch_size> [-c <cache_size>] [-r]" << std::endl;
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        std::cerr << "Usage: " << argv[0] << " -q <query_id> -p <parallelism> -b <batch_size> [-c <cache_size>] [-r]" << std::endl;
        exit(EXIT_SUCCESS);
    }
    // serializer lambda of TaxiRide
    auto ride_serializer = [](TaxiRide &ride) -> std::string
    {
        std::ostringstream oss;
        oss << ride.rideId << " "
            << std::put_time(&ride.time, "%Y-%m-%d %H:%M:%S") << " "
            << ride.isStart << " "
            << ride.location.lon << " "
            << ride.location.lat << " "
            << ride.passengerCnt << " "
            << ride.travelDist << " "
            << ride.area;
        return oss.str();
    };
    // deserializer lambda of TaxiRide
    auto ride_deserializer = [](std::string &s) -> TaxiRide
    {
        std::istringstream iss(s);
        TaxiRide ride;
        std::string timeStr;
        iss >> ride.rideId;
        iss >> std::get_time(&ride.time, "%Y-%m-%d %H:%M:%S");
        iss >> ride.isStart;
        iss >> ride.location.lon;
        iss >> ride.location.lat;
        iss >> ride.passengerCnt;
        iss >> ride.travelDist;
        iss >> ride.area;    
        return ride;
    };
    // set RocksDB configuration
    rocksdb::Options options;
    DBOptions::set_default_db_options(options);
    options.write_buffer_size = MEM_TABLE_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = NUM_MEM_TABLE;
    // application starting time
    unsigned long app_start_time_nsec = current_time_nsecs();
    std::cout << "Executing Taxi_Queries with parameters:" << std::endl;
    std::cout << "  * query identifier: " << query_id << std::endl;
    std::cout << "  * batch size: " << batch_size << std::endl;
    std::cout << "  * parallelism: " << parallelism << std::endl;
    std::cout << "  * isPersisent: " << isPersisent << std::endl;
    std::cout << "  * topology: source -> keyed_windows -> sink" << std::endl;
    if (isPersisent && cache_size == 0) {
    	std::cout << "  * execution with RocksDB state backend" << std::endl;
    }
    else if (isPersisent && cache_size > 0) {
    	std::cout << "  * execution with RocksDB state backend and state caching" << std::endl;
    }
    PipeGraph topology("taxi_queries", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    // create the operators
    TaxiSource_Functor taxi_source(dataset_path, batch_size, app_runtime_sec, app_start_time_nsec);
    Source source = Source_Builder(taxi_source)
            .withParallelism(parallelism)
            .withName("taxi_source")
            .withOutputBatchSize(batch_size)
            .build();
    MultiPipe &mp = topology.add_source(source);
    if (query_id == 1) { // Q1
        Win_Functor_Q1 win_functor_q1;
        if (!isPersisent) {
            Keyed_Windows kwins = Keyed_Windows_Builder(win_functor_q1)
                              .withName("win_op")
                              .withParallelism(parallelism)
                              .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                              .withTBWindows(seconds(q1_win_len_sec), seconds(q1_slide_len_sec))
                              .withOutputBatchSize(batch_size)
                              .build();
            mp.add(kwins);
        }
        else {
            P_Keyed_Windows kwins = P_Keyed_Windows_Builder(win_functor_q1)
                                        .withName("win_op_persistent")
                                        .withParallelism(parallelism)
                                        .withSharedDb(false)
                                        .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                                        .withTBWindows(seconds(q1_win_len_sec), seconds(q1_slide_len_sec))
                                        .withOptions(options)
                                        .setFragmentSize(FRAGMENT_SIZE)
                                        .withTupleSerializerAndDeserializer(ride_serializer, ride_deserializer)
                                        .withCacheCapacity(cache_size)
                                        .withOutputBatchSize(batch_size)
                                        .build();
            mp.add(kwins);
        }
    }
    else if (query_id == 2) { // Q2
        Win_Functor_Q2 win_functor_q2;
        if (!isPersisent) {
            Keyed_Windows kwins = Keyed_Windows_Builder(win_functor_q2)
                              .withName("win_op")
                              .withParallelism(parallelism)
                              .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                              .withTBWindows(seconds(q2_win_len_sec), seconds(q2_slide_len_sec))
                              .withOutputBatchSize(batch_size)
                              .build();
            mp.add(kwins);
        }
        else {
            P_Keyed_Windows kwins = P_Keyed_Windows_Builder(win_functor_q2)
                                        .withName("win_op_persistent")
                                        .withParallelism(parallelism)
                                        .withSharedDb(false)
                                        .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                                        .withTBWindows(seconds(q2_win_len_sec), seconds(q2_slide_len_sec))
                                        .withOptions(options)
                                        .setFragmentSize(FRAGMENT_SIZE)
                                        .withTupleSerializerAndDeserializer(ride_serializer, ride_deserializer)
                                        .withCacheCapacity(cache_size)
                                        .withOutputBatchSize(batch_size)
                                        .build();
            mp.add(kwins);
        }
    }
    else if (query_id == 3) { // Q3
        Win_Functor_Q3 win_functor_q3;
        if (!isPersisent) {
            Keyed_Windows kwins = Keyed_Windows_Builder(win_functor_q3)
                              .withName("win_op")
                              .withParallelism(parallelism)
                              .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                              .withTBWindows(seconds(q3_win_len_sec), seconds(q3_slide_len_sec))
                              .withOutputBatchSize(batch_size)
                              .build();
            mp.add(kwins);
        }
        else {
            P_Keyed_Windows kwins = P_Keyed_Windows_Builder(win_functor_q3)
                                        .withName("win_op_persistent")
                                        .withParallelism(parallelism)
                                        .withSharedDb(false)
                                        .withKeyBy([](const TaxiRide &r) -> std::string { return r.area; })
                                        .withTBWindows(seconds(q3_win_len_sec), seconds(q3_slide_len_sec))
                                        .withOptions(options)
                                        .setFragmentSize(FRAGMENT_SIZE)
                                        .withTupleSerializerAndDeserializer(ride_serializer, ride_deserializer)
                                        .withCacheCapacity(cache_size)
                                        .withOutputBatchSize(batch_size)
                                        .build();
            mp.add(kwins);
        }
    }
    else {
        std::cerr << "Unsupported query identifier" << std::endl;
        exit(EXIT_FAILURE);
    }
    TaxiSink_Functor taxi_sink(query_id);
    Sink sink = Sink_Builder(taxi_sink)
            .withParallelism(parallelism)
            .withName("taxi_sink")
            .build();
    mp.add_sink(sink);
    std::cout << "Executing topology" << std::endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    std::cout << "Exiting" << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    std::cout << "Measured throughput: " << (int) throughput << " tuples/second" << std::endl;
    return 0;
}
