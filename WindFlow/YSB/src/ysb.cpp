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

#include <regex>
#include <string>
#include <vector>
#include <iostream>
#include <ff/ff.hpp>
#include <windflow.hpp>

#include "../includes/nodes/sink.hpp"
#include "../includes/util/event.hpp"
#include "../includes/util/result.hpp"
#include "../includes/nodes/source.hpp"
#include "../includes/nodes/filter.hpp"
#include "../includes/nodes/joiner.hpp"
#include "../includes/util/cli_util.hpp"
#include "../includes/util/constants.hpp"
#include "../includes/util/joined_event.hpp"
#include "../includes/util/campaign_generator.hpp"

using namespace std;
using namespace ff;
using namespace wf;
using namespace chrono;

// global variables
atomic<long> sent_tuples;                    // total number of events processed by the system

// function for computing the final aggregates on tumbling windows (INCremental version)
void aggregateFunctionINC(const joined_event_t &event, result_t &result, RuntimeContext &rc)
{
    result.count++;
    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

// Main
int main(int argc, char* argv[]) {
    /// parse arguments from command line
    int option = 0;
    int index = 0;
    size_t source_par_deg = 0;
    size_t filter_par_deg = 0;
    size_t joiner_par_deg = 0;
    size_t winAgg_par_deg = 0;
    size_t sink_par_deg = 0;
    int rate = 0;
    sent_tuples = 0;
    long sampling = 0;
    bool chaining = false;
    size_t batch_size = 0;
    if (argc == 9 || argc == 10) {
        while ((option = getopt_long(argc, argv, "r:s:p:b:c:", long_opts, &index)) != -1) {
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
                    if (par_degs.size() != 5) {
                        printf("Error in parsing the input arguments\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        source_par_deg = par_degs[0];
                        filter_par_deg = par_degs[1];
                        joiner_par_deg = par_degs[2];
                        winAgg_par_deg = par_degs[3];
                        sink_par_deg = par_degs[4];
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
                    printf("Parameters: --rate <value> --sampling <value> --batch <size> --parallelism <nSource,nFilter,nJoiner,nWinAggregate,nSink> [--chaining]\n");
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        printf("Error in parsing the input arguments\n");
        exit(EXIT_FAILURE);
    }
    // create the campaigns
    CampaignGenerator campaign_gen;
    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    cout << "Executing YSB with parameters:" << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tupes/second" << endl;
    }
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * sampling: " << sampling << endl;
    cout << "  * source: " << source_par_deg << endl;
    cout << "  * filter: " << filter_par_deg << endl;
    cout << "  * joiner: " << joiner_par_deg << endl;
    cout << "  * winAggregate: " << winAgg_par_deg << endl;
    cout << "  * sink: " << sink_par_deg << endl;
    cout << "  * topology: source -> filter -> joiner -> winAggregate -> sink" << endl;
    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::INGRESS_TIME);
    if (!chaining) { // no chaining
        /// create the operators
        Source_Functor source_functor(rate, app_start_time, campaign_gen.getArrays(), campaign_gen.getAdsCompaign());
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .withOutputBatchSize(batch_size)
                .build();
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                .withParallelism(filter_par_deg)
                .withName(filter_name)
                .withOutputBatchSize(batch_size)
                .build();
        Joiner_Functor joiner_functor(campaign_gen.getHashMap(), campaign_gen.getRelationalTable());
        FlatMap joiner = FlatMap_Builder(joiner_functor)
                .withParallelism(joiner_par_deg)
                .withName(joiner_name)
                .withOutputBatchSize(batch_size)
                .build();
        Keyed_Windows winAggregate = Keyed_Windows_Builder(aggregateFunctionINC)
                .withTBWindows(seconds(10), seconds(10))
                .withName("yb_kf")
                .withParallelism(winAgg_par_deg)
                .withKeyBy([](const joined_event_t &in) -> unsigned long { return in.cmp_id; })
                .withOutputBatchSize(batch_size)
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is disabled" << endl;
        mp.add(filter);
        mp.add(joiner);
        mp.add(winAggregate);
        mp.add_sink(sink);
    }
    else { // chaining
        /// create the operators
        Source_Functor source_functor(rate, app_start_time, campaign_gen.getArrays(), campaign_gen.getAdsCompaign());
        Source source = Source_Builder(source_functor)
                .withParallelism(source_par_deg)
                .withName(source_name)
                .build();
        Filter_Functor filter_functor;
        Filter filter = Filter_Builder(filter_functor)
                .withParallelism(filter_par_deg)
                .withName(filter_name)
                .build();
        Joiner_Functor joiner_functor(campaign_gen.getHashMap(), campaign_gen.getRelationalTable());
        FlatMap joiner = FlatMap_Builder(joiner_functor)
                .withParallelism(joiner_par_deg)
                .withName(joiner_name)
                .withOutputBatchSize(batch_size)
                .build();
        Keyed_Windows winAggregate = Keyed_Windows_Builder(aggregateFunctionINC)
                .withTBWindows(seconds(10), seconds(10))
                .withName("yb_kf")
                .withParallelism(winAgg_par_deg)
                .withKeyBy([](const joined_event_t &in) -> unsigned long { return in.cmp_id; })
                .build();
        Sink_Functor sink_functor(sampling, app_start_time);
        Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName(sink_name)
                .build();
        MultiPipe &mp = topology.add_source(source);
        cout << "Chaining is enabled" << endl;
        mp.chain(filter);
        mp.chain(joiner);
        mp.add(winAggregate);
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
