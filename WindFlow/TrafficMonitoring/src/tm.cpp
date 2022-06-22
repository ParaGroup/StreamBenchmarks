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

#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

#include "../includes/util/tuple.hpp"
#include "../includes/nodes/sink.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/nodes/map_matcher.hpp"
#include "../includes/nodes/speed_calculator.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Type of Beijing input records:
// < vehicle_ID_value, n_ID_value, date_value, latitude_value, longitude_value, speed_value, direction_value >
using beijing_record_t = tuple<int, int, string, double, double, double, int>;

// Type of Dublin input records:
// < timestamp_value, line_ID_value, direction_value, journey_pattern_ID_value, time_frame_value, vehicle_journey_ID_value, operator_value,
// congestion_value, longitude_value, latitude_value, delay_value, block_ID_value, bus_ID_value, stop_ID_value, at_stop_value >
using dublin_record_t = tuple<long, int, int, string, string, int, string, int, double, double, int, int, int, int, int>;

// global variables
vector<beijing_record_t> beijing_parsed_file;           // contains data extracted from the Beijing input file
vector<dublin_record_t> dublin_parsed_file;             // contains data extracted from the Dublin input file
vector<tuple_t> dataset;                                // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;                // contains the number of occurrences of each key vehicle_id
Road_Grid_List road_grid_list;                          // contains data extracted from the city shapefile
atomic<long> sent_tuples;                               // total number of tuples sent by all the sources

/** 
 *  @brief Parse the input file.
 *  
 *  The file is parsed and saved in memory.
 *  @param file_path the path of the input dataset file
 */ 
void parse_dataset(const string& file_path) {
    ifstream file(file_path);
    if (file.is_open()) {
        size_t all_records = 0;         // counter of all records (dataset line) read
        size_t incomplete_records = 0;  // counter of the incomplete records
        string line;
        while (getline(file, line)) {
            // process file line
            int token_count = 0;
            vector<string> tokens;
            size_t last = 0;
            size_t next = 0;
            while ((next = line.find(',', last)) != string::npos) {
                tokens.push_back(line.substr(last, next - last));
                last = next + 1;
                token_count++;
            }
            tokens.push_back(line.substr(last));
            token_count++;
            // A record is valid if it contains at least 7 values (one for each field of interest)
            // in the case in which the application analyzes data coming from Beijing taxi-traces.
            if (_monitored_city == BEIJING) {
                if (token_count >= 7) {
                    // save parsed file
                    beijing_record_t r(atoi(tokens.at(TAXI_ID_FIELD).c_str()),
                                       atoi(tokens.at(NID_FIELD).c_str()),
                                       tokens.at(DATE_FIELD),
                                       atof(tokens.at(TAXI_LATITUDE_FIELD).c_str()),
                                       atof(tokens.at(TAXI_LONGITUDE_FIELD).c_str()),
                                       atof(tokens.at(TAXI_SPEED_FIELD).c_str()),
                                       atoi(tokens.at(TAXI_DIRECTION_FIELD).c_str()));
                    beijing_parsed_file.push_back(r);

                    // insert the key device_id in the map (if it is not present)
                    if (key_occ.find(get<TAXI_ID_FIELD>(r)) == key_occ.end()) {
                        key_occ.insert(make_pair(get<TAXI_ID_FIELD>(r), 0));
                    }
                }
                else
                    incomplete_records++;
            }
            else if (_monitored_city == DUBLIN) {
                // A record is valid if it contains at least 15 values (one for each field of interest)
                // in the case in which the application analyzes data coming from Dublin bus-traces.
                if (token_count >= 15) {
                    // save parsed file
                    dublin_record_t r(atol(tokens.at(TIMESTAMP_FIELD).c_str()),
                                      atoi(tokens.at(LINE_ID_FIELD).c_str()),
                                      atoi(tokens.at(BUS_DIRECTION_FIELD).c_str()),
                                      tokens.at(JOURNEY_PATTERN_ID_FIELD),
                                      tokens.at(TIME_FRAME_FIELD),
                                      atoi(tokens.at(VEHICLE_JOURNEY_ID_FIELD).c_str()),
                                      tokens.at(OPERATOR_FIELD),
                                      atoi(tokens.at(CONGESTION_FIELD).c_str()),
                                      atof(tokens.at(BUS_LONGITUDE_FIELD).c_str()),
                                      atof(tokens.at(BUS_LATITUDE_FIELD).c_str()),
                                      atoi(tokens.at(DELAY_FIELD).c_str()),
                                      atoi(tokens.at(BLOCK_ID_FIELD).c_str()),
                                      atoi(tokens.at(BUS_ID_FIELD).c_str()),
                                      atoi(tokens.at(STOP_ID_FIELD).c_str()),
                                      atoi(tokens.at(AT_STOP_ID_FIELD).c_str()));
                    dublin_parsed_file.push_back(r);

                    // insert the key device_id in the map (if it is not present)
                    if (key_occ.find(get<BUS_ID_FIELD>(r)) == key_occ.end()) {
                        key_occ.insert(make_pair(get<BUS_ID_FIELD>(r), 0));
                    }
                }
                else
                    incomplete_records++;
            }

            all_records++;
        }
        file.close();
        //if (_monitored_city == BEIJING) print_taxi_parsing_info(beijing_parsed_file, all_records, incomplete_records);
        //else if (_monitored_city == DUBLIN) print_bus_parsing_info(dublin_parsed_file, all_records, incomplete_records);
    }
}

/** 
 *  @brief Process parsed data and create all the tuples.
 *  
 *  The created tuples are maintained in memory. The source node will generate the stream by
 *  reading all the tuples from main memory.
 */ 
void create_tuples() {
    if (_monitored_city == BEIJING) {
        for (int next_tuple_idx = 0; next_tuple_idx < beijing_parsed_file.size(); next_tuple_idx++) {
            // create tuple
            beijing_record_t record = beijing_parsed_file.at(next_tuple_idx);
            tuple_t t;
            t.latitude = get<TAXI_LATITUDE_FIELD>(record);
            t.longitude = get<TAXI_LONGITUDE_FIELD>(record);
            t.speed = get<TAXI_SPEED_FIELD>(record);
            t.direction = get<TAXI_DIRECTION_FIELD>(record);
            t.key = get<TAXI_ID_FIELD>(record);
            //t.id = (key_occ.find(get<TAXI_ID_FIELD>(record)))->second++;
            //t.ts = 0L;
            dataset.insert(dataset.end(), t);
        }
    }
    else if (_monitored_city == DUBLIN) {
        for (int next_tuple_idx = 0; next_tuple_idx < dublin_parsed_file.size(); next_tuple_idx++) {
            // create tuple
            dublin_record_t record = dublin_parsed_file.at(next_tuple_idx);
            tuple_t t;
            t.latitude = get<BUS_LATITUDE_FIELD>(record);
            t.longitude = get<BUS_LONGITUDE_FIELD>(record);
            t.speed = 0.0; // speed values are not present in the used dataset
            t.direction = get<BUS_DIRECTION_FIELD>(record);
            t.key = get<BUS_ID_FIELD>(record);
            //t.id = (key_occ.find(get<BUS_ID_FIELD>(record)))->second++;
            //t.ts = 0L;
            dataset.insert(dataset.end(), t);
        }
    }
}

/** 
 *  @brief Parse the shapefile and create a the Road_Grid_List data structure.
 *  
 *  The data structure containing the processed information about the roads of the city
 *  is passed to the MapMatcher node and use to implement the map matching logic.
 */ 
void read_shapefile() {
    string shapefile_path = (_monitored_city == DUBLIN) ? _dublin_shapefile : _beijing_shapefile;
    if (road_grid_list.read_shapefile(shapefile_path) == -1)
        __throw_invalid_argument("Failed reading shapefile");
}

// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    string file_path;
    size_t source_par_deg = 0;
    size_t matcher_par_deg = 0;
    size_t calculator_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    sent_tuples = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    if (argc == 9 || argc == 10) {
        while ((option = getopt_long(argc, argv, "r:s:p:b:c:", long_opts, &index)) != -1) {
            file_path = _beijing_input_file;
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 's': {
                    sampling = atoi(optarg);
                    break;
                }
                case 'b': {
                    batch_size = atoi(optarg);
                    break;
                }
                case 'p': {
                    vector<size_t> par_degs;
                    string pars(optarg);
                    stringstream ss(pars);
                    for (size_t i; ss >> i;) {
                        par_degs.push_back(i);
                        if (ss.peek() == ',')
                            ss.ignore();
                    }
                    if (par_degs.size() != 4) {
                        printf("Error in parsing the input arguments\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        source_par_deg = par_degs[0];
                        matcher_par_deg = par_degs[1];
                        calculator_par_deg = par_degs[2];
                        sink_par_deg = par_degs[3];
                    }
                    break;
                }
                case 'c': {
                    chaining = true;
                    break;
                }
                default: {
                    printf("Error in parsing the input arguments\n");
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else if (argc == 2) {
        while ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) {
            switch (option) {
                case 'h': {
                    printf("Parameters: --rate <value> --sampling <value> --batch <size> --parallelism <nSource,nMap-Matcher,nSpeed-Calculator,nSink> [--chaining]\n");
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        printf("Error in parsing the input arguments\n");
        exit(EXIT_FAILURE);
    }
    /// data pre-processing
    parse_dataset(file_path);
    create_tuples();
    read_shapefile();
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    cout << "Executing TrafficMonitoring with parameters:" << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tupes/second" << endl;
    }
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * map-matcher: " << matcher_par_deg << endl;
    cout << "  * speed-calculator: " << calculator_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> map-matcher -> speed-calculator -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::INGRESS_TIME);
    if (!chaining) { // no chaining
        /// create the nodes
        Source_Functor source_functor(dataset, rate, app_start_time);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .withOutputBatchSize(batch_size)
                .build();
        Map_Matcher_Functor map_match_functor(road_grid_list, app_start_time);
        FlatMap map_matcher = FlatMap_Builder(map_match_functor)
                .withParallelism(matcher_par_deg)
                .withName(map_match_name)
                .withOutputBatchSize(batch_size)
                .build();
        Speed_Calculator_Functor speed_calc_functor(app_start_time);
        Map speed_calculator = Map_Builder(speed_calc_functor)
                .withParallelism(calculator_par_deg)
                .withName(speed_calc_name)
                .withKeyBy([](const result_t &r) -> size_t { return r.key; })
                .withOutputBatchSize(batch_size)
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();       
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is disabled" << endl;
        mp.add(map_matcher);
        mp.add(speed_calculator);
        mp.add_sink(sink);      
    }
    else { // chaining
        /// create the nodes
        Source_Functor source_functor(dataset, rate, app_start_time);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .build();
        Map_Matcher_Functor map_match_functor(road_grid_list, app_start_time);
        FlatMap map_matcher = FlatMap_Builder(map_match_functor)
                .withParallelism(matcher_par_deg)
                .withName(map_match_name)
                .withOutputBatchSize(batch_size)
                .build();
        Speed_Calculator_Functor speed_calc_functor(app_start_time);
        Map speed_calculator = Map_Builder(speed_calc_functor)
                .withParallelism(calculator_par_deg)
                .withName(speed_calc_name)
                .withKeyBy([](const result_t &r) -> size_t { return r.key; })
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();       
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is enabled" << endl;
        mp.chain(map_matcher);
        mp.chain(speed_calculator);
        mp.chain_sink(sink);     
    }
    cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    cout << "Exiting" << endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    return 0;
}
