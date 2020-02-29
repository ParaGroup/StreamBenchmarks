#include "util/configuration.hpp"
#include "util/drain_sink.hpp"
#include "util/line_reader_source.hpp"
#include "util/metric_group.hpp"
#include "util/topology.hpp"
#include "voip_stream/acd.hpp"
#include "voip_stream/ct24.hpp"
#include "voip_stream/dispatcher.hpp"
#include "voip_stream/ecr.hpp"
#include "voip_stream/ecr24.hpp"
#include "voip_stream/ecr_reordering.hpp"
#include "voip_stream/encr.hpp"
#include "voip_stream/encr_reordering.hpp"
#include "voip_stream/fofir.hpp"
#include "voip_stream/fofir_forwarding.hpp"
#include "voip_stream/fofir_reordering.hpp"
#include "voip_stream/global_acd.hpp"
#include "voip_stream/parser.hpp"
#include "voip_stream/pre_rcr.hpp"
#include "voip_stream/rcr.hpp"
#include "voip_stream/score.hpp"
#include "voip_stream/url.hpp"
#include "voip_stream/url_forwarding.hpp"
#include <cstdio>
#include <string>
#include <windflow.hpp>
#include <atomic>

// global variable for throughput
std::atomic<unsigned long> sent_tuples;

template <typename Source, typename Sink>
static void run_default_variant(wf::PipeGraph &graph, Source &source, Sink &sink, const util::Configuration &configuration, bool chaining)
{
    // build nodes

    auto parser = util::setup(
        "parser",
        configuration,
        wf::Map_Builder(voip_stream::Parser())
    ).build();

    auto dispatcher = util::setup(
        "dispatcher",
        configuration,
        wf::Map_Builder(voip_stream::Dispatcher())
    ).enable_KeyBy().build();

    auto ct24 = util::setup(
        "ct24",
        configuration,
        wf::FlatMap_Builder(voip_stream::CT24())
    ).enable_KeyBy().build();

    // XXX fixed parallelism degree
    auto global_acd = wf::Map_Builder(voip_stream::GlobalACD()).build();

    auto ecr24 = util::setup(
        "ecr24",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR24())
    ).enable_KeyBy().build();

    auto acd = util::setup(
        "acd",
        configuration,
        wf::FlatMap_Builder(voip_stream::ACD())
    ).enable_KeyBy().build();

    auto pre_rcr = util::setup(
        "pre_rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::PreRCR())
    ).build();

    auto rcr = util::setup(
        "rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::RCR())
    ).enable_KeyBy().build();

    auto ecr1 = util::setup(
        "ecr_1",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR())
    ).enable_KeyBy().build();

    auto ecr2 = util::setup(
        "ecr_2",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR())
    ).enable_KeyBy().build();

    auto encr = util::setup(
        "encr",
        configuration,
        wf::FlatMap_Builder(voip_stream::ENCR())
    ).enable_KeyBy().build();

    auto fofir = util::setup(
        "fofir",
        configuration,
        wf::FlatMap_Builder(voip_stream::FoFiR())
    ).enable_KeyBy().build();

    auto url = util::setup(
        "url",
        configuration,
        wf::FlatMap_Builder(voip_stream::URL())
    ).enable_KeyBy().build();

    auto score = util::setup(
        "score",
        configuration,
        wf::Map_Builder(voip_stream::Score())
    ).enable_KeyBy().build();

    // build topology

    if (chaining) {
        std::cout << "Chaining is enabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.chain(parser);
        dispatcher_pipe.chain(dispatcher);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4, 5, 6};
            return broadcast;
        }, 7);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.chain(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.chain(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.chain(ecr24);

        wf::MultiPipe &rcr_pipe = dispatcher_pipe.select(3);
        rcr_pipe.chain(pre_rcr);
        rcr_pipe.chain(rcr);

        wf::MultiPipe &ecr1_pipe = dispatcher_pipe.select(4);
        ecr1_pipe.chain(ecr1);

        wf::MultiPipe &ecr2_pipe = dispatcher_pipe.select(5);
        ecr2_pipe.chain(ecr2);

        wf::MultiPipe &encr_pipe = dispatcher_pipe.select(6);
        encr_pipe.chain(encr);

        //

        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.chain(acd);

        wf::MultiPipe &fofir_pipe = rcr_pipe.merge(ecr1_pipe);
        fofir_pipe.chain(fofir);

        wf::MultiPipe &url_pipe = ecr2_pipe.merge(encr_pipe);
        url_pipe.chain(url);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(fofir_pipe, url_pipe);
        sink_pipe.chain(score);
        sink_pipe.chain_sink(sink);
    } else {
        std::cout << "Chaining is disabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.add(parser);
        dispatcher_pipe.add(dispatcher);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4, 5, 6};
            return broadcast;
        }, 7);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.add(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.add(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.add(ecr24);

        wf::MultiPipe &rcr_pipe = dispatcher_pipe.select(3);
        rcr_pipe.add(pre_rcr);
        rcr_pipe.add(rcr);

        wf::MultiPipe &ecr1_pipe = dispatcher_pipe.select(4);
        ecr1_pipe.add(ecr1);

        wf::MultiPipe &ecr2_pipe = dispatcher_pipe.select(5);
        ecr2_pipe.add(ecr2);

        wf::MultiPipe &encr_pipe = dispatcher_pipe.select(6);
        encr_pipe.add(encr);

        //

        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.add(acd);

        wf::MultiPipe &fofir_pipe = rcr_pipe.merge(ecr1_pipe);
        fofir_pipe.add(fofir);

        wf::MultiPipe &url_pipe = ecr2_pipe.merge(encr_pipe);
        url_pipe.add(url);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(fofir_pipe, url_pipe);
        sink_pipe.add(score);
        sink_pipe.add_sink(sink);
    }
    std::cout << "Executing topology" << std::endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = wf::current_time_usecs();
    // start!
    graph.run();
    volatile unsigned long end_time_main_usecs = wf::current_time_usecs();
    std::cout << "Exiting" << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    std::cout << "Measured throughput: " << (int) throughput << " tuples/second" << std::endl;
    std::cout << "Dumping metrics" << std::endl;
    util::metric_group.dump_all();
}

template <typename Source, typename Sink>
static void run_reordering_variant(wf::PipeGraph &graph, Source &source, Sink &sink, const util::Configuration &configuration, bool chaining)
{
    // build nodes

    auto parser = util::setup(
        "parser",
        configuration,
        wf::Map_Builder(voip_stream::Parser())
    ).build();

    auto dispatcher = util::setup(
        "dispatcher",
        configuration,
        wf::Map_Builder(voip_stream::Dispatcher())
    ).enable_KeyBy().build();

    auto ecr = util::setup(
        "ecr",
        configuration,
        wf::Map_Builder(voip_stream::reordering::ECR())
    ).enable_KeyBy().build();

    auto ct24 = util::setup(
        "ct24",
        configuration,
        wf::FlatMap_Builder(voip_stream::CT24())
    ).enable_KeyBy().build();

    // XXX fixed parallelism degree
    auto global_acd = wf::Map_Builder(voip_stream::GlobalACD()).build();

    auto ecr24 = util::setup(
        "ecr24",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR24())
    ).enable_KeyBy().build();

    auto acd = util::setup(
        "acd",
        configuration,
        wf::FlatMap_Builder(voip_stream::ACD())
    ).enable_KeyBy().build();

    auto pre_rcr = util::setup(
        "pre_rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::PreRCR())
    ).build();

    auto rcr = util::setup(
        "rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::RCR())
    ).enable_KeyBy().build();

    auto encr = util::setup(
        "encr",
        configuration,
        wf::FlatMap_Builder(voip_stream::reordering::ENCR())
    ).enable_KeyBy().build();

    auto fofir = util::setup(
        "fofir",
        configuration,
        wf::FlatMap_Builder(voip_stream::reordering::FoFiR())
    ).enable_KeyBy().build();

    auto url = util::setup(
        "url",
        configuration,
        wf::FlatMap_Builder(voip_stream::URL())
    ).enable_KeyBy().build();

    auto score = util::setup(
        "score",
        configuration,
        wf::Map_Builder(voip_stream::Score())
    ).enable_KeyBy().build();

    // build topology

    if (chaining) {
        std::cout << "Chaining is enabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.chain(parser);
        dispatcher_pipe.chain(dispatcher);
        dispatcher_pipe.chain(ecr);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4};
            return broadcast;
        }, 5);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.chain(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.chain(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.chain(ecr24);

        //

        wf::MultiPipe &fofir_pipe = dispatcher_pipe.select(3);
        fofir_pipe.chain(pre_rcr);
        fofir_pipe.chain(rcr);
        fofir_pipe.chain(fofir);

        wf::MultiPipe &url_pipe = dispatcher_pipe.select(4);
        url_pipe.chain(encr);
        url_pipe.chain(url);

        // XXX this must be *after* all the select()s
        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.chain(acd);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(fofir_pipe, url_pipe);
        sink_pipe.chain(score);
        sink_pipe.chain_sink(sink);
    } else {
        std::cout << "Chaining is disabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.add(parser);
        dispatcher_pipe.add(dispatcher);
        dispatcher_pipe.add(ecr);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4};
            return broadcast;
        }, 5);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.add(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.add(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.add(ecr24);

        //

        wf::MultiPipe &fofir_pipe = dispatcher_pipe.select(3);
        fofir_pipe.add(pre_rcr);
        fofir_pipe.add(rcr);
        fofir_pipe.add(fofir);

        wf::MultiPipe &url_pipe = dispatcher_pipe.select(4);
        url_pipe.add(encr);
        url_pipe.add(url);

        // XXX this must be *after* all the select()s
        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.add(acd);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(fofir_pipe, url_pipe);
        sink_pipe.add(score);
        sink_pipe.add_sink(sink);
    }
    std::cout << "Executing topology" << std::endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = wf::current_time_usecs();
    graph.run();
    volatile unsigned long end_time_main_usecs = wf::current_time_usecs();
    std::cout << "Exiting" << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    std::cout << "Measured throughput: " << (int) throughput << " tuples/second" << std::endl;
    std::cout << "Dumping metrics" << std::endl;
    util::metric_group.dump_all();
}

template <typename Source, typename Sink>
static void run_forwarding_variant(wf::PipeGraph &graph, Source &source, Sink &sink, const util::Configuration &configuration, bool chaining)
{
    // build nodes

    auto parser = util::setup(
        "parser",
        configuration,
        wf::Map_Builder(voip_stream::Parser())
    ).build();

    auto dispatcher = util::setup(
        "dispatcher",
        configuration,
        wf::Map_Builder(voip_stream::Dispatcher())
    ).enable_KeyBy().build();

    auto ct24 = util::setup(
        "ct24",
        configuration,
        wf::FlatMap_Builder(voip_stream::CT24())
    ).enable_KeyBy().build();

    // XXX fixed parallelism degree
    auto global_acd = wf::Map_Builder(voip_stream::GlobalACD()).build();

    auto ecr24 = util::setup(
        "ecr24",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR24())
    ).enable_KeyBy().build();

    auto acd = util::setup(
        "acd",
        configuration,
        wf::FlatMap_Builder(voip_stream::ACD())
    ).enable_KeyBy().build();

    auto pre_rcr = util::setup(
        "pre_rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::PreRCR())
    ).build();

    auto rcr = util::setup(
        "rcr",
        configuration,
        wf::FlatMap_Builder(voip_stream::RCR())
    ).enable_KeyBy().build();

    auto ecr = util::setup(
        "ecr",
        configuration,
        wf::FlatMap_Builder(voip_stream::ECR())
    ).enable_KeyBy().build();

    auto encr = util::setup(
        "encr",
        configuration,
        wf::FlatMap_Builder(voip_stream::ENCR())
    ).enable_KeyBy().build();

    auto fofir = util::setup(
        "fofir",
        configuration,
        wf::FlatMap_Builder(voip_stream::forwarding::FoFiR())
    ).enable_KeyBy().build();

    auto url = util::setup(
        "url",
        configuration,
        wf::FlatMap_Builder(voip_stream::forwarding::URL())
    ).enable_KeyBy().build();

    auto score = util::setup(
        "score",
        configuration,
        wf::Map_Builder(voip_stream::Score())
    ).enable_KeyBy().build();

    // build topology

    if (chaining) {
        std::cout << "Chaining is enabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.chain(parser);
        dispatcher_pipe.chain(dispatcher);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4, 5};
            return broadcast;
        }, 6);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.chain(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.chain(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.chain(ecr24);

        wf::MultiPipe &rcr_pipe = dispatcher_pipe.select(3);
        rcr_pipe.chain(pre_rcr);
        rcr_pipe.chain(rcr);

        wf::MultiPipe &ecr_pipe = dispatcher_pipe.select(4);
        ecr_pipe.chain(ecr);

        wf::MultiPipe &encr_pipe = dispatcher_pipe.select(5);
        encr_pipe.chain(encr);

        //

        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.chain(acd);

        wf::MultiPipe &fofir_pipe = rcr_pipe.merge(ecr_pipe);
        fofir_pipe.chain(fofir);

        wf::MultiPipe &url_pipe = fofir_pipe.merge(encr_pipe);
        url_pipe.chain(url);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(url_pipe);
        sink_pipe.chain(score);
        sink_pipe.chain_sink(sink);
    } else {
        std::cout << "Chaining is disabled" << std::endl;
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        dispatcher_pipe.add(parser);
        dispatcher_pipe.add(dispatcher);

        dispatcher_pipe.split([] (const voip_stream::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2, 3, 4, 5};
            return broadcast;
        }, 6);

        //

        wf::MultiPipe &ct24_pipe = dispatcher_pipe.select(0);
        ct24_pipe.add(ct24);

        wf::MultiPipe &global_acd_pipe = dispatcher_pipe.select(1);
        global_acd_pipe.add(global_acd);

        wf::MultiPipe &ecr24_pipe = dispatcher_pipe.select(2);
        ecr24_pipe.add(ecr24);

        wf::MultiPipe &rcr_pipe = dispatcher_pipe.select(3);
        rcr_pipe.add(pre_rcr);
        rcr_pipe.add(rcr);

        wf::MultiPipe &ecr_pipe = dispatcher_pipe.select(4);
        ecr_pipe.add(ecr);

        wf::MultiPipe &encr_pipe = dispatcher_pipe.select(5);
        encr_pipe.add(encr);

        //

        wf::MultiPipe &acd_pipe = ct24_pipe.merge(global_acd_pipe, ecr24_pipe);
        acd_pipe.add(acd);

        wf::MultiPipe &fofir_pipe = rcr_pipe.merge(ecr_pipe);
        fofir_pipe.add(fofir);

        wf::MultiPipe &url_pipe = fofir_pipe.merge(encr_pipe);
        url_pipe.add(url);

        //

        wf::MultiPipe &sink_pipe = acd_pipe.merge(url_pipe);
        sink_pipe.add(score);
        sink_pipe.add_sink(sink);
    }
    std::cout << "Executing topology" << std::endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = wf::current_time_usecs();
    graph.run();
    volatile unsigned long end_time_main_usecs = wf::current_time_usecs();
    std::cout << "Exiting" << std::endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    std::cout << "Measured throughput: " << (int) throughput << " tuples/second" << std::endl;
    std::cout << "Dumping metrics" << std::endl;
    util::metric_group.dump_all();
}

int main(int argc, char *argv[])
{
    auto configuration = util::Configuration::from_args(argc, argv);
    auto dataset_path = configuration.get_tree()["dataset"].GetString();
    auto run_time = configuration.get_tree()["run_time"].GetInt();
    auto gen_rate = configuration.get_tree()["gen_rate"].GetInt();
    auto sampling_rate = configuration.get_tree()["sampling_rate"].GetInt();
    auto chaining = configuration.get_tree()["chaining"].GetBool();
    auto variant = std::string(configuration.get_tree()["variant"].GetString());
    // initialize global variable for throughput
    sent_tuples = 0;

    std::cout << "Executing VoipStream (" + variant + ") with parameters:" << std::endl;
    if (gen_rate != 0)
        std::cout << "  * rate: " << gen_rate << " tuples/second" << std::endl;
    else
        std::cout << "  * rate: full_speed tupes/second" << std::endl;
    std::cout << "  * sampling: " << sampling_rate << std::endl;
    if (variant == "default")
        std::cout << "  * topology: complex with 16 operators" << std::endl;
    else
        std::cout << "  * topology: complex with 15 operators" << std::endl;

    // build source and sink nodes

    auto source = util::setup(
        "source",
        configuration,
        wf::Source_Builder(util::LineReaderSource(run_time, gen_rate, dataset_path))
    ).build();

    auto sink = util::setup(
        "sink",
        configuration,
        wf::Sink_Builder(util::DrainSink<voip_stream::ScoreTuple>(sampling_rate))
    ).build();

    // build topology
    wf::PipeGraph graph(argv[0]);
    if (variant == "default") { // duplication
        run_default_variant(graph, source, sink, configuration, chaining);
    } else if (variant == "reordering") {
        run_reordering_variant(graph, source, sink, configuration, chaining);
    } else if (variant == "forwarding") {
        run_forwarding_variant(graph, source, sink, configuration, chaining);
    } else {
        std::cerr << "Unknown variant\n";
        std::exit(1);
    }
}
