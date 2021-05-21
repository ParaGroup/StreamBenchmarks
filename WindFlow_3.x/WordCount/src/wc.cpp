/** 
 *  @file    wc.cpp
 *  @author  Gabriele Mencagli
 *  @date    13/01/2020
 *  
 *  @brief Main of the WordCount application
 */ 

#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

#include "../includes/util/tuple.hpp"
#include "../includes/nodes/sink.hpp"
#include "../includes/util/result.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/nodes/counter.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/nodes/splitter.hpp"
#include "../includes/util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// global variables
vector<tuple_t> dataset;                    // contains all the input tuples in memory
atomic<long> total_lines;                   // total number of lines processed by the system
atomic<long> total_bytes;                   // total number of bytes processed by the system

/** 
 *  @brief Parse the input file and create all the tuples
 *  
 *  The created tuples are maintained in memory. The source node will generate the stream by
 *  reading all the tuples from main memory.
 *  
 *  @param file_path the path of the input dataset file
 */ 
void parse_dataset_and_create_tuples(const string& file_path) {
    ifstream file(file_path);
    if (file.is_open()) {
        size_t all_records = 0;         // counter of all records (dataset line) read
        string line;
        while (getline(file, line)) {
            // process file line
            if (!line.empty()) {
                tuple_t t(line, all_records);
                all_records++;
                dataset.push_back(t);
            }
        }
        file.close();
    }
}

// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    string file_path;
    size_t source_par_deg = 0;
    size_t splitter_par_deg = 0;
    size_t counter_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    total_lines = 0;
    total_bytes = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    if (argc == 9 || argc == 10) {
        while ((option = getopt_long(argc, argv, "r:s:p:b:c:", long_opts, &index)) != -1) {
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
                        splitter_par_deg = par_degs[1];
                        counter_par_deg = par_degs[2];
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
                    printf("Parameters: --rate <value> --sampling <value> --batch <size> --parallelism <nSource,nSplitter,nCounter,nSink> [--chaining]\n");
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
    parse_dataset_and_create_tuples(file_path);
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    cout << "Executing WordCount with parameters:" << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tupes/second" << endl;
    }
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * splitter: " << splitter_par_deg << endl;
    cout << "  * counter: " << counter_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> splitter -> counter -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::INGRESS_TIME);
    if (!chaining) { // no chaining
        /// create the operators
        Source_Functor source_functor(dataset, rate, app_start_time);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .withOutputBatchSize(batch_size)
                .build();
        Splitter_Functor splitter_functor(app_start_time);
        FlatMap splitter = FlatMap_Builder(splitter_functor)
                .withParallelism(splitter_par_deg)
                .withName(splitter_name)
                .withOutputBatchSize(batch_size)
                .build();
        Counter_Functor counter_functor(app_start_time);
        Accumulator counter = Accumulator_Builder(counter_functor)
                .withParallelism(counter_par_deg)
                .withName(counter_name)
                .withKeyBy([](const result_t &r) -> std::string { return r.key; })
                .withInitialState(result_t())
                .withOutputBatchSize(batch_size)
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is disabled" << endl;
        mp.add(splitter);
        mp.add(counter);
        mp.add_sink(sink);      
    }
    else {
        /// create the operators
        Source_Functor source_functor(dataset, rate, app_start_time);
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .build();
        Splitter_Functor splitter_functor(app_start_time);
        FlatMap splitter = FlatMap_Builder(splitter_functor)
                .withParallelism(splitter_par_deg)
                .withName(splitter_name)
                .withOutputBatchSize(batch_size)
                .build();
        Counter_Functor counter_functor(app_start_time);
        Accumulator counter = Accumulator_Builder(counter_functor)
                .withParallelism(counter_par_deg)
                .withName(counter_name)
                .withKeyBy([](const result_t &r) -> std::string { return r.key; })
                .withInitialState(result_t())
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is enabled" << endl;
        mp.chain(splitter);
        mp.add(counter);
        mp.chain_sink(sink);        
    }
    cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    cout << "Exiting" << endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = total_lines / elapsed_time_seconds;
    double mbs = (double)((total_bytes / 1048576) / elapsed_time_seconds);
    cout << "Measured throughput: " << (int) throughput << " lines/second, " << mbs << " MB/s" << endl;
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    return 0;
}
