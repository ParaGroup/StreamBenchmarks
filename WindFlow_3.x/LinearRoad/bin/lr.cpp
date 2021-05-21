#include "linear_road/average_speed.hpp"
#include "linear_road/count_vehicles.hpp"
#include "linear_road/dispatcher.hpp"
#include "linear_road/last_average_speed.hpp"
#include "linear_road/toll_notification_cv.hpp"
#include "linear_road/toll_notification_las.hpp"
#include "linear_road/toll_notification_pos.hpp"
#include "util/configuration.hpp"
#include "util/drain_sink.hpp"
#include "util/line_reader_source.hpp"
#include "util/metric_group.hpp"
#include "util/topology.hpp"
#include <windflow.hpp>
#include <atomic>

// global variable for throughput
std::atomic<unsigned long> sent_tuples;

int main(int argc, char *argv[])
{
    auto configuration = util::Configuration::from_args(argc, argv);
    auto dataset_path = configuration.get_tree()["dataset"].GetString();
    auto run_time = configuration.get_tree()["run_time"].GetInt();
    auto gen_rate = configuration.get_tree()["gen_rate"].GetInt();
    auto sampling_rate = configuration.get_tree()["sampling_rate"].GetInt();
    auto chaining = configuration.get_tree()["chaining"].GetBool();
    // initialize global variable for throughput
    sent_tuples = 0;
    // build source and sink nodes
    unsigned long app_start_time = wf::current_time_nsecs();
    std::cout << "Executing LinearRoad with parameters:" << std::endl;
    if (gen_rate != 0) {
        std::cout << "  * rate: " << gen_rate << " tuples/second" << std::endl;
    }
    else {
        std::cout << "  * rate: full_speed tupes/second" << std::endl;
    }
    std::cout << "  * batch size: " << configuration.batch_size << std::endl;
    std::cout << "  * sampling: " << sampling_rate << std::endl;
    std::cout << "  * topology: complex with 9 operators" << std::endl;
    wf::PipeGraph graph(argv[0], wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::INGRESS_TIME);
    if (!chaining) { // no chaining
        auto source = util::setup(
            "source",
            configuration,
            wf::Source_Builder(util::LineReaderSource(run_time, gen_rate, dataset_path))
        ).withOutputBatchSize(configuration.batch_size).build();
        auto dispatcher = util::setup(
            "dispatcher",
            configuration,
            wf::FlatMap_Builder(linear_road::Dispatcher())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto toll_notification_pos = util::setup(
            "toll_notification_pos",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationPos())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto average_speed = util::setup(
            "average_speed",
            configuration,
            wf::FlatMap_Builder(linear_road::AverageSpeed())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto last_average_speed = util::setup(
            "last_average_speed",
            configuration,
            wf::FlatMap_Builder(linear_road::LastAverageSpeed())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto toll_notification_las = util::setup(
            "toll_notification_las",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationLas())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto count_vehicles = util::setup(
            "count_vehicles",
            configuration,
            wf::FlatMap_Builder(linear_road::CountVehicles())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto toll_notification_cv = util::setup(
            "toll_notification_cv",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationCv())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto sink = util::setup(
            "sink",
            configuration,
            wf::Sink_Builder(util::DrainSink<linear_road::NotificationTuple>(sampling_rate))).build();
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        std::cout << "Chaining is disabled" << std::endl;
        dispatcher_pipe.add(dispatcher);
        dispatcher_pipe.split([] (const linear_road::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2};
            return broadcast;
        }, 3);
        wf::MultiPipe &position_pipe = dispatcher_pipe.select(0);
        position_pipe.add(toll_notification_pos);
        wf::MultiPipe &speed_pipe = dispatcher_pipe.select(1);
        speed_pipe.add(average_speed);
        speed_pipe.add(last_average_speed);
        speed_pipe.add(toll_notification_las);
        wf::MultiPipe &count_pipe = dispatcher_pipe.select(2);
        count_pipe.add(count_vehicles);
        count_pipe.add(toll_notification_cv);
        wf::MultiPipe &sink_pipe = position_pipe.merge(speed_pipe, count_pipe);
        sink_pipe.add_sink(sink);
    }
    else { // chaining
        auto source = util::setup(
            "source",
            configuration,
            wf::Source_Builder(util::LineReaderSource(run_time, gen_rate, dataset_path))
        ).build();
        auto dispatcher = util::setup(
            "dispatcher",
            configuration,
            wf::FlatMap_Builder(linear_road::Dispatcher())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto toll_notification_pos = util::setup(
            "toll_notification_pos",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationPos())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto average_speed = util::setup(
            "average_speed",
            configuration,
            wf::FlatMap_Builder(linear_road::AverageSpeed())
        ).build();
        auto last_average_speed = util::setup(
            "last_average_speed",
            configuration,
            wf::FlatMap_Builder(linear_road::LastAverageSpeed())
        ).build();
        auto toll_notification_las = util::setup(
            "toll_notification_las",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationLas())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto count_vehicles = util::setup(
            "count_vehicles",
            configuration,
            wf::FlatMap_Builder(linear_road::CountVehicles())
        ).build();
        auto toll_notification_cv = util::setup(
            "toll_notification_cv",
            configuration,
            wf::FlatMap_Builder(linear_road::TollNotificationCv())
        ).withOutputBatchSize(configuration.batch_size).build();
        auto sink = util::setup(
            "sink",
            configuration,
            wf::Sink_Builder(util::DrainSink<linear_road::NotificationTuple>(sampling_rate))).build();
        wf::MultiPipe &dispatcher_pipe = graph.add_source(source);
        std::cout << "Chaining is disabled" << std::endl;
        dispatcher_pipe.add(dispatcher);
        dispatcher_pipe.split([] (const linear_road::Dispatcher::Tuple &tuple) {
            static const std::vector<size_t> broadcast = {0, 1, 2};
            return broadcast;
        }, 3);
        wf::MultiPipe &position_pipe = dispatcher_pipe.select(0);
        position_pipe.chain(toll_notification_pos);
        wf::MultiPipe &speed_pipe = dispatcher_pipe.select(1);
        speed_pipe.chain(average_speed);
        speed_pipe.chain(last_average_speed);
        speed_pipe.chain(toll_notification_las);
        wf::MultiPipe &count_pipe = dispatcher_pipe.select(2);
        count_pipe.chain(count_vehicles);
        count_pipe.chain(toll_notification_cv);
        wf::MultiPipe &sink_pipe = position_pipe.merge(speed_pipe, count_pipe);
        sink_pipe.chain_sink(sink);
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
