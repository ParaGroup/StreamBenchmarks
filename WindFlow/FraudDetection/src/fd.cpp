/** 
 *  @file    fd.cpp
 *  @author  Gabriele Mencagli
 *  @date    11/01/2020
 *  
 *  @brief Main of the FraudDetection application
 */ 

#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

#include "../includes/nodes/sink.hpp"
#include "../includes/util/tuple.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/predictor.hpp"
#include "../includes/nodes/light_source.hpp"

using namespace std;
using namespace ff;
using namespace wf;

using count_key_t = pair<size_t, uint64_t>;
using key_map_t = unordered_map<string, count_key_t>;

// global variables
key_map_t entity_key_map;                   // contains a mapping between string keys and integer keys for each entity_id
size_t entity_unique_key = 0;               // unique integer key
vector<pair<string, string>> parsed_file;   // contains strings extracted from the input file
vector<tuple_t> dataset;                    // contains all the tuples in memory
atomic<long> sent_tuples;                   // total number of tuples sent by all the sources

/** 
 *  @brief Map keys and parse the input file
 *  
 *  This method assigns to each string key entity_id a unique integer key (required by the current
 *  implementation of WindFlow). Moreover, the file is parsed and saved in memory.
 *  
 *  @param file_path the path of the input dataset file
 *  @param split_regex the regular expression used to split the lines of the file
 *         (e.g. for a file input.csv the regular expression to be used is ",")
 */ 
void map_and_parse_dataset(const string& file_path, const string& split_regex) {
    ifstream file(file_path);
    if (file.is_open()) {
        string line;
        while (getline(file, line)) {
            string entity_id = line.substr(0, line.find(split_regex));
            string record = line.substr(line.find(split_regex) + 1, line.size());
            // map keys
            if (entity_key_map.find(entity_id) == entity_key_map.end()) { // the key is not present in the hash map
                entity_key_map.insert(make_pair(entity_id, make_pair(entity_unique_key, 0)));
                entity_unique_key++;
            }
            // save parsed file
            parsed_file.insert(parsed_file.end(), make_pair(entity_id, record));
        }
        file.close();
    }
}

/** 
 *  @brief Process parsed data and create all the tuples
 *  
 *  The created tuples are maintained in memory. The source node will generate the stream by
 *  reading all the tuples from main memory.
 */ 
void create_tuples() {
    for (int next_tuple_idx = 0; next_tuple_idx < parsed_file.size(); next_tuple_idx++) {
        // create tuple
        auto tuple_content = parsed_file.at(next_tuple_idx);
        tuple_t t;
        t.entity_id = tuple_content.first;
        t.record = tuple_content.second;
        t.key = (entity_key_map.find(t.entity_id)->second).first;
        t.id = ((entity_key_map.find(t.entity_id))->second).second++;
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
    size_t predictor_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    sent_tuples = 0;
    long sampling = 0;
    bool chaining = false;
    if (argc == 7 || argc == 8) {
        while ((option = getopt_long(argc, argv, "r:s:p:c:", long_opts, &index)) != -1) {
            file_path = _input_file;
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 's': {
                    sampling = atoi(optarg);
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
                    if (par_degs.size() != 3) {
                        printf("Error in parsing the input arguments\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        source_par_deg = par_degs[0];
                        predictor_par_deg = par_degs[1];
                        sink_par_deg = par_degs[2];
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
                    printf("Parameters: --rate <value> --sampling <value> --parallelism <nSource,nPredictor,nSink> [--chaining]\n");
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
    map_and_parse_dataset(file_path, ",");
    create_tuples();
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();

    /// create the nodes
    Source_Functor source_functor(dataset, rate, app_start_time);
    Source source = Source_Builder(source_functor)
            .withParallelism(source_par_deg)
            .withName(light_source_name)
            .build();

    Predictor_Functor predictor_functor(app_start_time);
    FlatMap predictor = FlatMap_Builder(predictor_functor)
            .withParallelism(predictor_par_deg)
            .withName(predictor_name)
            .enable_KeyBy()
            .build();

    Sink_Functor sink_functor(sampling, app_start_time);
    Sink sink = Sink_Builder(sink_functor)
            .withParallelism(sink_par_deg)
            .withName(sink_name)
            .build();

    cout << "Executing FraudDetection with parameters:" << endl;
    if (rate != 0)
        cout << "  * rate: " << rate << " tuples/second" << endl;
    else
        cout << "  * rate: full_speed tupes/second" << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * predictor: " << predictor_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> predictor -> sink" << endl;

    /// create the application
    PipeGraph topology(topology_name);
    MultiPipe &mp = topology.add_source(source);
    if (chaining) {
        cout << "Chaining is enabled" << endl;
        mp.chain(predictor);
        mp.chain_sink(sink);
    }
    else {
        cout << "Chaining is disabled" << endl;
        mp.add(predictor);
        mp.add_sink(sink);
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
