/** 
 *  @file    sd.cpp
 *  @author  Gabriele Mencagli
 *  @date    13/01/2020
 *  
 *  @brief Main of the SpikeDetection application
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
#include "../includes/nodes/detector.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/nodes/average_calculator_map.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// type of the input records: < date_value, time_value, epoch_value, device_id_value, temp_value, humid_value, light_value, voltage_value>
using record_t = tuple<string, string, int, int, double, double, double, double>;

// global variables
vector<record_t> parsed_file;               // contains data extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
unordered_map<size_t, uint64_t> key_occ;    // contains the number of occurrences of each key device_id
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources

/** 
 *  @brief Parse the input file
 *  
 *  The file is parsed and saved in memory.
 *  
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
            regex rgx("\\s+"); // regex quantifier (matches one or many whitespaces)
            sregex_token_iterator iter(line.begin(), line.end(), rgx, -1);
            sregex_token_iterator end;
            while (iter != end) {
                tokens.push_back(*iter);
                token_count++;
                iter++;
            }
            // a record is valid if it contains at least 8 values (one for each field of interest)
            if (token_count >= 8) {
                // save parsed file
                record_t r(tokens.at(DATE_FIELD),
                           tokens.at(TIME_FIELD),
                           atoi(tokens.at(EPOCH_FIELD).c_str()),
                           atoi(tokens.at(DEVICE_ID_FIELD).c_str()),
                           atof(tokens.at(TEMP_FIELD).c_str()),
                           atof(tokens.at(HUMID_FIELD).c_str()),
                           atof(tokens.at(LIGHT_FIELD).c_str()),
                           atof(tokens.at(VOLT_FIELD).c_str()));
                parsed_file.push_back(r);
                // insert the key device_id in the map (if it is not present)
                if (key_occ.find(get<DEVICE_ID_FIELD>(r)) == key_occ.end()) {
                    key_occ.insert(make_pair(get<DEVICE_ID_FIELD>(r), 0));
                }
            }
            else
                incomplete_records++;

            all_records++;
        }
        file.close();
        //print_parsing_info(parsed_file, all_records, incomplete_records);
    }
}

/** 
 *  @brief Process parsed data and create all the tuples
 *  
 *  The created tuples are maintained in memory. The source node will generate the stream by
 *  reading all the tuples from main memory.
 */ 
void create_tuples(int num_keys)
{
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, num_keys-1);
    mt19937 rng;
    rng.seed(0);
    for (int next_tuple_idx = 0; next_tuple_idx < parsed_file.size(); next_tuple_idx++) {
        // create tuple
        auto record = parsed_file.at(next_tuple_idx);
        tuple_t t;
        // select the value of the field the user chose to monitor (parameter set in constants.hpp)
        if (_field == TEMPERATURE) {
            t.property_value = get<TEMP_FIELD>(record);
        }
        else if (_field == HUMIDITY) {
            t.property_value = get<HUMID_FIELD>(record);
        }
        else if (_field == LIGHT) {
            t.property_value = get<LIGHT_FIELD>(record);
        }
        else if (_field == VOLTAGE) {
            t.property_value = get<VOLT_FIELD>(record);
        }
        t.incremental_average = 0;
        if (num_keys > 0) {
            t.key = dist(rng);
        }
        else {
            t.key = get<DEVICE_ID_FIELD>(record);
        }
        t.ts = 0L;
        dataset.insert(dataset.end(), t);
    }
}

// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    string file_path;
    size_t source_par_deg = 0;
    size_t average_par_deg = 0;
    size_t detector_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    sent_tuples = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    int num_keys = 0;
    if (argc == 11 || argc == 12) {
        while ((option = getopt_long(argc, argv, "r:k:s:p:b:c:", long_opts, &index)) != -1) {
            file_path = _input_file;
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 'k': {
                    num_keys = atoi(optarg);
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
                        average_par_deg = par_degs[1];
                        detector_par_deg = par_degs[2];
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
                    printf("Parameters: --rate <value> --keys <value> --sampling <value> --batch <size> --parallelism <nSource,nMoving-Average,nSpike-Detector,nSink> [--chaining]\n");
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
    create_tuples(num_keys);
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    cout << "Executing SpikeDetection with parameters:" << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tuples/second" << endl;
    }
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * moving-average: " << average_par_deg << endl;
    cout << "  * spike-detector: " << detector_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> moving-average -> spike-detector -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    if (!chaining) { // no chaining
        /// create the operators
        Source_Functor source_functor(dataset, rate, app_start_time, batch_size);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .withOutputBatchSize(batch_size)
                .build();
        Average_Calculator_Map_Functor avg_calc_functor(app_start_time);
        Map average_calculator = Map_Builder(avg_calc_functor)
                .withParallelism(average_par_deg)
                .withName(avg_calc_name)
                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                .withOutputBatchSize(batch_size)
                .build();
        Detector_Functor detector_functor(app_start_time);
        Filter detector = Filter_Builder(detector_functor)
                .withParallelism(detector_par_deg)
                .withName(detector_name)
                .withOutputBatchSize(batch_size)
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is disabled" << endl;
        mp.add(average_calculator);
        mp.add(detector);
        mp.add_sink(sink);
    }
    else { // chaining
        /// create the operators
        Source_Functor source_functor(dataset, rate, app_start_time, batch_size);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .withOutputBatchSize(batch_size)
                .build();
        Average_Calculator_Map_Functor avg_calc_functor(app_start_time);
        Map average_calculator = Map_Builder(avg_calc_functor)
                .withParallelism(average_par_deg)
                .withName(avg_calc_name)
                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                .build();
        Detector_Functor detector_functor(app_start_time);
        Filter detector = Filter_Builder(detector_functor)
                .withParallelism(detector_par_deg)
                .withName(detector_name)
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is enabled" << endl;
        mp.chain(average_calculator);
        mp.chain(detector);
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
