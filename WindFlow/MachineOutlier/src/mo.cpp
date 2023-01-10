/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Cosimo Agati
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

#include<algorithm>
#include<atomic>
#include<cassert>
#include<cmath>
#include<cstdlib>
#include<ctime>
#include<deque>
#include<fstream>
#include<getopt.h>
#include<iostream>
#include<limits>
#include<mutex>
#include<nlohmann/json.hpp>
#include<numeric>
#include<optional>
#include<queue>
#include<sstream>
#include<string>
#include<string_view>
#include<unistd.h>
#include<unordered_map>
#include<utility>
#include<valarray>
#include<vector>
#include "../includes/util.hpp"
#include<windflow.hpp>

using namespace std;
using namespace wf;

/*
 * Indices identifying the operators in the data flow graph.
 */
enum NodeId : unsigned
{
    source_id          = 0,
    observer_id        = 1,
    anomaly_scorer_id  = 2,
    alert_triggerer_id = 3,
    sink_id            = 4,
    num_nodes          = 5
};

/*
 * This struct holds every command line parameter.
 */
struct Parameters
{
    const char *     metric_output_directory = ".";
    const char *     anomaly_scorer_type     = "data-stream";
    const char *     alert_triggerer_type    = "top-k";
    const char *     parser_type             = "alibaba";
    const char *     input_file              = "../../Datasets/MO/machine-usage.csv";
    Execution_Mode_t execution_mode          = Execution_Mode_t::DETERMINISTIC;
    Time_Policy_t    time_policy             = Time_Policy_t::EVENT_TIME;
    unsigned         parallelism[num_nodes]  = {1, 1, 1, 1, 1};
    unsigned         duration                  = 60;
    unsigned         tuple_rate                = 0;
    unsigned         sampling_rate             = 100;
    bool             use_chaining              = false;
};

struct MachineMetadata
{
    string        machine_ip;
    double        cpu_usage;
    double        memory_usage;
    double        score;
    unsigned long timestamp;
};

#ifndef NDEBUG
ostream &operator<<(ostream &stream, const MachineMetadata &metadata)
{
    stream << "{Machine IP: " << metadata.machine_ip
           << ", CPU usage: " << metadata.cpu_usage
           << ", memory usage: " << metadata.memory_usage
           << ", observation timestamp: " << metadata.timestamp << '}';
    return stream;
}
#endif

template<typename T>
struct ScorePackage
{
    string id;
    double score;
    T      data;
};

template<typename T>
struct StreamProfile
{
    string id;
    T      current_data_instance;
    double stream_anomaly_score;
    double current_data_instance_score;
};

struct SourceTuple
{
    MachineMetadata observation;
    unsigned long   ordering_timestamp;
    unsigned long   execution_timestamp;
};

struct ObservationResultTuple
{
    string          id;
    double          score;
    unsigned long   ordering_timestamp;
    unsigned long   parent_execution_timestamp;
    MachineMetadata observation;
};

struct AnomalyResultTuple
{
    string          id;
    double          anomaly_score;
    unsigned long   ordering_timestamp;
    unsigned long   parent_execution_timestamp;
    MachineMetadata observation;
    double          individual_score;
};

bool operator<(AnomalyResultTuple a, AnomalyResultTuple b)
{
    return a.anomaly_score < b.anomaly_score;
}

struct AlertTriggererResultTuple
{
    string          id;
    double          anomaly_score;
    unsigned long   parent_execution_timestamp;
    bool            is_abnormal;
    MachineMetadata observation;
};

template<typename Tuple>
struct TimestampGreaterComparator
{
    bool operator()(const Tuple &t, const Tuple &s) {
        return t.ordering_timestamp > s.ordering_timestamp;
    }
};

template<typename T>
using TimestampPriorityQueue = priority_queue<T, vector<T>, TimestampGreaterComparator<T>>;
template<typename Scorer>
struct ObservationScorerData
{
    Scorer                              scorer;
    TimestampPriorityQueue<SourceTuple> tuple_queue;
    vector<MachineMetadata>             observation_list;
    unsigned long                       previous_ordering_timestamp = 0;
    unsigned long                       parent_execution_timestamp;
    Execution_Mode_t                    execution_mode;
    Shipper<ObservationResultTuple> *   shipper;
};

template<typename T>
struct DataStreamAnomalyScorerData
{
    unordered_map<string, StreamProfile<T>>        stream_profile_map;
    TimestampPriorityQueue<ObservationResultTuple> tuple_queue;
    bool                                           shrink_next_round = false;
    unsigned long                previous_ordering_timestamp         = 0;
    unsigned long                parent_execution_timestamp          = 0;
    Execution_Mode_t             execution_mode;
    Shipper<AnomalyResultTuple> *shipper;
};

struct SlidingWindowStreamAnomalyScorerData
{
    unordered_map<string, deque<double>>           sliding_window_map;
    TimestampPriorityQueue<ObservationResultTuple> tuple_queue;
    Execution_Mode_t                               execution_mode;
    size_t                                         window_length;
    unsigned long previous_timestamp = 0; // XXX: is this needed?
    Shipper<AnomalyResultTuple> *shipper;
};

struct AlertTriggererData
{
    inline static const double dupper = sqrt(2);
    unsigned long                              previous_ordering_timestamp = 0;
    unsigned long                              parent_execution_timestamp  = 0;
    vector<AnomalyResultTuple>                 stream_list;
    TimestampPriorityQueue<AnomalyResultTuple> tuple_queue;
    double           min_data_instance_score = numeric_limits<double>::max();
    double           max_data_instance_score = 0.0;
    Execution_Mode_t execution_mode;
    Shipper<AlertTriggererResultTuple> *shipper;
};

struct TopKAlertTriggererData
{
    vector<AnomalyResultTuple>                 stream_list;
    TimestampPriorityQueue<AnomalyResultTuple> tuple_queue;
    size_t                                     k;
    unsigned long                              previous_ordering_timestamp = 0;
    unsigned long                              parent_execution_timestamp  = 0;
    Execution_Mode_t                           execution_mode;
    Shipper<AlertTriggererResultTuple> *       shipper;
};

static const struct option long_opts[] = {{"help", 0, 0, 'h'},
                                          {"rate", 1, 0, 'r'},
                                          {"sampling", 1, 0, 's'},
                                          {"parallelism", 1, 0, 'p'},
                                          {"batch", 1, 0, 'b'},
                                          {"chaining", 1, 0, 'c'},
                                          {"duration", 1, 0, 'd'},
                                          {"execmode", 1, 0, 'e'},
                                          {"timepolicy", 1, 0, 't'},
                                          {"outputdir", 1, 0, 'o'},
                                          {"anomalyscorer", 1, 0, 'a'},
                                          {"alerttriggerer", 1, 0, 'g'},
                                          {"file", 1, 0, 'f'},
                                          {"parser", 1, 0, 'P'},
                                          {0, 0, 0, 0}};

static inline optional<MachineMetadata> parse_google_trace(const string &trace)
{
    const auto      values             = string_split(trace, ',');
    const size_t    timestamp_index    = 0;
    const size_t    machine_id_index   = 4;
    const size_t    cpu_usage_index    = 5;
    const size_t    memory_usage_index = 6;
    MachineMetadata metadata;

    if (values.size() != 19) {
        return {};
    }

    metadata.machine_ip   = values[machine_id_index];
    metadata.timestamp    = stoul(values[timestamp_index].data());
    metadata.cpu_usage    = stod(values[cpu_usage_index].data()) * 10;
    metadata.memory_usage = stod(values[memory_usage_index].data()) * 10;
    return metadata;
}

static inline optional<MachineMetadata> parse_alibaba_trace(const string &trace)
{
    const auto values = string_split(trace, ',');
    if (values.size() != 7) {
        return {};
    }

    const size_t    timestamp_index    = 1;
    const size_t    machine_id_index   = 0;
    const size_t    cpu_usage_index    = 2;
    const size_t    memory_usage_index = 3;
    MachineMetadata metadata;

    metadata.machine_ip   = values[machine_id_index];
    metadata.timestamp    = stoul(values[timestamp_index].data()) * 1000;
    metadata.cpu_usage    = stod(values[cpu_usage_index].data());
    metadata.memory_usage = stod(values[memory_usage_index].data());
    return metadata;
}

static inline double eucledean_norm(const valarray<double> &elements)
{
    double result = 0.0;
    for (const auto &x : elements) {
        result += pow(x, 2.0);
    }
    return sqrt(result);
}

template<optional<MachineMetadata> parse_trace(const string &)>
static inline vector<MachineMetadata> parse_metadata(const char *filename)
{
    ifstream                metadata_stream {filename};
    vector<MachineMetadata> metadata_info;

    for (string line; getline(metadata_stream, line);) {
        auto metadata = parse_trace(line);
        if (metadata) {
            metadata_info.push_back(move(*metadata));
        }
    }
    return metadata_info;
}

static inline void parse_args(int argc, char **argv, Parameters &parameters)
{
    int option;
    int index;
    parameters.use_chaining = false;
    if (argc == 7 || argc == 8) {
        while ((option = getopt_long(argc, argv, "r:s:p:c", long_opts, &index)) != -1) {
            switch (option) {
                case 'r':
                    parameters.tuple_rate = atoi(optarg);
                    break;
                case 's':
                    parameters.sampling_rate = atoi(optarg);
                    break;
                case 'p': {
                    const auto degrees = get_nums_split_by_commas(optarg);
                    if (degrees.size() != num_nodes) {
                        cerr << "Error in parsing the input arguments.  Parallelism "
                                "degree string requires exactly "
                             << num_nodes << " elements.\n";
                        exit(EXIT_FAILURE);
                    }
                    else {
                        for (unsigned i = 0; i < num_nodes; ++i) {
                            parameters.parallelism[i] = degrees[i];
                        }
                    }
                } break;
                case 'c':
                    parameters.use_chaining = true;
                    break;
                default: {
                    cerr << "Error in parsing the input arguments" << endl;
                    exit(EXIT_FAILURE);
                } break;
            }
        }
    }
    else if (argc == 2) {
        while ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) {
            switch (option) {
                case 'h': {
                    cout << "Parameters: --rate <value> --sampling <value> --parallelism <nSource,nObservationScorer,nAnomalyScorer,nAlertTriggerer,nSink> [--chaining]" << endl;
                    exit(EXIT_SUCCESS);
                }
            }
        }
    }
    else {
        cerr << "Error in parsing the input arguments" << endl;
        exit(EXIT_FAILURE);
    }
}

static inline void validate_args(const Parameters &parameters)
{
    if (parameters.duration == 0) {
        cerr << "Error: duration must be positive\n";
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < num_nodes; ++i) {
        if (parameters.parallelism[i] == 0) {
            cerr << "Error: parallelism degree for node " << i
                 << " must be positive\n";
            exit(EXIT_FAILURE);
        }
    }
}

static inline void print_initial_parameters(const Parameters &parameters)
{
    cout << "Executing MachineOutlier with parameters:" << endl;
    if (parameters.tuple_rate != 0) {
        cout << "  * rate: " << parameters.tuple_rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tupes/second" << endl;
    }
    cout << "  * sampling: " << parameters.sampling_rate << endl;
    cout << "  * source: " << parameters.parallelism[source_id] << endl;
    cout << "  * observation-scorer: " << parameters.parallelism[observer_id] << endl;
    cout << "  * anomaly-scorer: " << parameters.parallelism[anomaly_scorer_id] << endl;
    cout << "  * alert-triggerer: " << parameters.parallelism[alert_triggerer_id] << endl;
    cout << "  * sink: " << parameters.parallelism[sink_id] << endl;
    cout << "  * topology: source -> observation-scorer -> anomaly-scorer -> alert-triggerer -> sink" << endl;
    if (!parameters.use_chaining) {
        cout << "Chaining is disabled" << endl;
    }
    else {
        cout << "Chaining is enabled" << endl;
    }
    cout << "Executing topology" << endl;
}

/*
 * Global variables
 */
static atomic_ulong          global_sent_tuples {0};
static atomic_ulong          global_received_tuples {0};
static Metric<unsigned long> global_latency_metric {"metric_latency"};
#ifndef NDEBUG
static mutex print_mutex;
#endif

template<typename Observation, optional<Observation> parse_trace(const string &)>
class SourceFunctor
{
    vector<Observation>   observations;
    Execution_Mode_t      execution_mode;
    unsigned long         measurement_timestamp_additional_amount = 0;
    unsigned long         measurement_timestamp_increase_step;
    unsigned long         duration;
    unsigned              tuple_rate_per_second;

public:
    SourceFunctor(unsigned d, unsigned rate, Execution_Mode_t e, const char *path):
                  observations {parse_metadata<parse_trace>(path)},
                  execution_mode {e},
                  duration {d * timeunit_scale_factor},
                  tuple_rate_per_second {rate}
    {
        if (observations.empty()) {
            cerr << "Error: empty machine reading stream.  Check whether "
                    "dataset file exists and is readable\n";
            exit(EXIT_FAILURE);
        }
    }

    void operator()(Source_Shipper<SourceTuple> &shipper, RuntimeContext &context)
    {
        const unsigned long end_time    = current_time() + duration;
        unsigned long       sent_tuples = 0;
        size_t              index       = 0;
        DO_NOT_WARN_IF_UNUSED(context);

        while (current_time() < end_time) {
            auto current_observation = observations[index];
            current_observation.timestamp +=
                measurement_timestamp_additional_amount;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SOURCE " << context.getReplicaIndex()
                     << "] Sending out tuple with the following "
                        "observation: "
                     << current_observation << '\n'
                     << "[SOURCE " << context.getReplicaIndex()
                     << "] Shipper address: " << &shipper
                     << " Runtime Context address: " << &context << '\n';
            }
#endif
            index = (index + 1) % observations.size();
            if (index == 0) {
                if (measurement_timestamp_additional_amount == 0) {
                    measurement_timestamp_increase_step =
                        current_observation.timestamp;
                }
                measurement_timestamp_additional_amount +=
                    measurement_timestamp_increase_step;
            }

            const unsigned long execution_timestamp = current_time();

            SourceTuple new_tuple = {current_observation,
                                     current_observation.timestamp,
                                     execution_timestamp};

            shipper.pushWithTimestamp(move(new_tuple),
                                      new_tuple.observation.timestamp);
            if (execution_mode == Execution_Mode_t::DEFAULT) {
                shipper.setNextWatermark(new_tuple.observation.timestamp);
            }
            ++sent_tuples;
            if (tuple_rate_per_second > 0) {
                const unsigned long delay =
                    (1.0 / tuple_rate_per_second) * timeunit_scale_factor;
                busy_wait(delay);
            }
        }
        global_sent_tuples.fetch_add(sent_tuples);
    }
};

class MachineMetadataScorer
{
    static constexpr size_t cpu_idx    = 0;
    static constexpr size_t memory_idx = 1;

    valarray<double> calculate_distance(valarray<valarray<double>> &matrix) const
    {
        assert(matrix.size() > 0);
#ifndef NDEBUG
        for (const auto &row : matrix) {
            assert(row.size() > 0);
        }
#endif
        valarray<double> mins(matrix[0].size());
        valarray<double> maxs(matrix[0].size());
        const auto       column_number = matrix[0].size();

        for (size_t col = 0; col < column_number; ++col) {
            double min = numeric_limits<double>::min();
            double max = numeric_limits<double>::max();

            for (size_t row {0}; row < matrix.size(); ++row) {
                if (matrix[row][col] < min) {
                    min = matrix[row][col];
                }
                if (matrix[row][col] > min) {
                    max = matrix[row][col];
                }
            }
            mins[col] = min;
            maxs[col] = max;
        }
        mins[cpu_idx] = 0.0;
        maxs[cpu_idx] = 1.0;

        mins[memory_idx] = 0.0;
        maxs[memory_idx] = 100.0;

        valarray<double> centers(0.0, column_number);
        for (size_t col = 0; col < column_number; ++col) {
            if (mins[col] == 0 && maxs[col] == 0) {
                continue;
            }
            for (size_t row = 0; row < matrix.size(); ++row) {
                matrix[row][col] =
                    (matrix[row][col] - mins[col]) / (maxs[col] - mins[col]);
                centers[col] += matrix[row][col];
            }
            centers[col] /= matrix.size();
        }

        valarray<valarray<double>> distances(
            valarray<double>(0.0, matrix[0].size()), matrix.size());

        for (size_t row = 0; row < matrix.size(); ++row) {
            for (size_t col {0}; col < matrix[row].size(); ++col) {
                distances[row][col] = abs(matrix[row][col] - centers[col]);
            }
        }

        valarray<double> l2distances(matrix.size());
        for (size_t row = 0; row < l2distances.size(); ++row) {
            l2distances[row] = eucledean_norm(distances[row]);
        }
        return l2distances;
    }

public:
    vector<ScorePackage<MachineMetadata>> get_scores(const vector<MachineMetadata> &observation_list) const
    {
        vector<ScorePackage<MachineMetadata>> score_package_list;

        valarray<valarray<double>> matrix(valarray<double>(0.0, 2),
                                          observation_list.size());

        for (size_t i = 0; i < observation_list.size(); ++i) {
            const auto &metadata  = observation_list[i];
            matrix[i][cpu_idx]    = metadata.cpu_usage;
            matrix[i][memory_idx] = metadata.memory_usage;
        }

        const auto l2distances = calculate_distance(matrix);
        for (size_t i = 0; i < observation_list.size(); ++i) {
            auto &                        metadata = observation_list[i];
            ScorePackage<MachineMetadata> package {
                metadata.machine_ip, 1.0 + l2distances[i], move(metadata)};
            score_package_list.push_back(move(package));
        }

        return score_package_list;
    }
};

template<typename Scorer>
void process_observations(const SourceTuple &tuple, RuntimeContext &context)
{
#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[OBSERVATION SCORER " << context.getReplicaIndex()
             << "] Processing tuple with ordering timestamp: "
             << tuple.ordering_timestamp
             << ", WindFlow timestamp: " << context.getCurrentTimestamp()
             << '\n';
    }
#endif
    assert(context.getLocalStorage().isContained("data"));
    auto &data =
        context.getLocalStorage().get<ObservationScorerData<Scorer>>("data");
    assert(tuple.ordering_timestamp >= data.previous_ordering_timestamp);

    if (tuple.ordering_timestamp > data.previous_ordering_timestamp) {
        if (!data.observation_list.empty()) {
            const auto score_package_list =
                data.scorer.get_scores(data.observation_list);
            const unsigned long next_ordering_timestamp =
                data.execution_mode == Execution_Mode_t::DEFAULT
                    ? context.getLastWatermark()
                    : data.previous_ordering_timestamp;

            for (const auto &package : score_package_list) {
                ObservationResultTuple result {
                    package.id, package.score, next_ordering_timestamp,
                    data.parent_execution_timestamp, package.data};
#ifndef NDEBUG
                {
                    lock_guard lock {print_mutex};
                    clog << "[OBSERVATION SCORER " << context.getReplicaIndex()
                         << "] Sending tuple with id: " << result.id
                         << ", score: " << result.score
                         << ", ordering timestamp: "
                         << result.ordering_timestamp
                         << ", observation: " << result.observation
                         << ", current WindFlow timestamp: "
                         << context.getCurrentTimestamp() << '\n';
                }
#endif
                data.shipper->push(move(result));
            }
            data.observation_list.clear();
        }
        data.previous_ordering_timestamp = tuple.ordering_timestamp;
    }

    if (data.observation_list.empty()) {
        data.parent_execution_timestamp = tuple.execution_timestamp;
    }
    data.observation_list.push_back(tuple.observation);
}

template<typename Data, typename Input, void process(const Input &, RuntimeContext &)>
void process_last_tuples(RuntimeContext &context)
{
    auto &storage = context.getLocalStorage();
    if (storage.isContained("data")) {
        auto &data        = storage.get<Data>("data");
        auto &tuple_queue = data.tuple_queue;

        for (; !tuple_queue.empty(); tuple_queue.pop()) {
            process(tuple_queue.top(), context);
        }
        storage.remove<Data>("data");
    }
}

template<typename Scorer>
class ObservationScorerFunctor
{
    Execution_Mode_t execution_mode;

public:
    ObservationScorerFunctor(Execution_Mode_t e): execution_mode {e} {}

    void operator()(const SourceTuple &              tuple,
                    Shipper<ObservationResultTuple> &shipper,
                    RuntimeContext &                 context)
    {
        const unsigned long watermark = context.getLastWatermark();
        assert(tuple.ordering_timestamp >= watermark);
        auto &storage = context.getLocalStorage();
        if (!storage.isContained("data")) {
            auto &data = storage.get<ObservationScorerData<Scorer>>("data");
            data.execution_mode = execution_mode;
            data.shipper        = &shipper;
        }
        auto &tuple_queue =
            storage.get<ObservationScorerData<Scorer>>("data").tuple_queue;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[OBSERVATION SCORER " << context.getReplicaIndex()
                 << "] Received tuple with ordering timestamp: "
                 << tuple.ordering_timestamp
                 << ", WindFlow timestamp: " << context.getCurrentTimestamp()
                 << ", current amount of tuples cached: " << tuple_queue.size()
                 << ", current watermark: " << watermark << '\n';
        }
#endif
        switch (execution_mode) {
        case Execution_Mode_t::DETERMINISTIC:
            process_observations<Scorer>(tuple, context);
            break;
        case Execution_Mode_t::DEFAULT:
            tuple_queue.push(tuple);

            while (!tuple_queue.empty()
                   && tuple_queue.top().ordering_timestamp <= watermark) {
                process_observations<Scorer>(tuple_queue.top(), context);
                tuple_queue.pop();
            }
            break;
        default:
            cerr << "[OBSERVATION SCORER] Error: unknown execution mode\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
};

template<typename T>
void process_data_stream_anomalies(const ObservationResultTuple &tuple,
                                   RuntimeContext &              context)
{
    static constexpr double lambda    = 0.017;
    static const double     factor    = exp(-lambda);
    static const double     threshold = 1 / (1 - factor) * 0.5;

#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[ANOMALY SCORER " << context.getReplicaIndex()
             << "] Processing tuple containing observation: "
             << tuple.observation
             << ", ordering timestamp: " << tuple.ordering_timestamp
             << ", WindFlow timestamp: " << context.getCurrentTimestamp()
             << '\n';
    }
#endif
    assert(context.getLocalStorage().isContained("data"));
    auto &data =
        context.getLocalStorage().get<DataStreamAnomalyScorerData<T>>("data");
    assert(tuple.ordering_timestamp >= data.previous_ordering_timestamp);

    if (tuple.ordering_timestamp > data.previous_ordering_timestamp) {
        const unsigned long next_ordering_timestamp =
            data.execution_mode == Execution_Mode_t::DEFAULT
                ? context.getLastWatermark()
                : data.previous_ordering_timestamp;

        for (auto &entry : data.stream_profile_map) {
            auto &stream_profile = entry.second;
            if (data.shrink_next_round) {
                stream_profile.stream_anomaly_score = 0;
            }

            AnomalyResultTuple result {
                entry.first,
                stream_profile.stream_anomaly_score,
                next_ordering_timestamp,
                data.parent_execution_timestamp,
                stream_profile.current_data_instance,
                stream_profile.current_data_instance_score,
            };
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ANOMALY SCORER " << context.getReplicaIndex()
                     << "] Sending out tuple with observation: "
                     << result.observation
                     << ", score sum: " << result.anomaly_score
                     << ", individual score: " << result.individual_score
                     << ", ordering timestamp: " << result.ordering_timestamp
                     << ", WindFlow timestamp: "
                     << context.getCurrentTimestamp() << '\n';
            }
#endif
            data.shipper->push(move(result));
        }

        if (data.shrink_next_round) {
            data.shrink_next_round = false;
        }
        data.previous_ordering_timestamp = tuple.ordering_timestamp;
        data.parent_execution_timestamp  = tuple.parent_execution_timestamp;
    }

    const auto profile_entry = data.stream_profile_map.find(tuple.id);

    if (profile_entry == data.stream_profile_map.end()) {
        StreamProfile<T> profile {tuple.id, tuple.observation, tuple.score,
                                  tuple.score};
        data.stream_profile_map.insert({tuple.id, move(profile)});
    }
    else {
        auto &profile = profile_entry->second;
        profile.stream_anomaly_score =
            profile.stream_anomaly_score * factor + tuple.score;
        profile.current_data_instance       = tuple.observation;
        profile.current_data_instance_score = tuple.score;

        if (profile.stream_anomaly_score > threshold) {
            data.shrink_next_round = true;
        }
        data.stream_profile_map.insert_or_assign(tuple.id, profile);
    }
}

template<typename T>
class DataStreamAnomalyScorerFunctor
{
    Execution_Mode_t execution_mode;

public:
    DataStreamAnomalyScorerFunctor(Execution_Mode_t e): execution_mode {e} {}

    void operator()(const ObservationResultTuple &tuple,
                    Shipper<AnomalyResultTuple> & shipper,
                    RuntimeContext &              context)
    {
        const unsigned long watermark = context.getLastWatermark();
        assert(tuple.ordering_timestamp >= watermark);
        auto &storage = context.getLocalStorage();
        if (!storage.isContained("data")) {
            auto &data = storage.get<DataStreamAnomalyScorerData<T>>("data");
            data.execution_mode = execution_mode;
            data.shipper        = &shipper;
        }
        auto &tuple_queue =
            storage.get<DataStreamAnomalyScorerData<T>>("data").tuple_queue;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ANOMALY SCORER " << context.getReplicaIndex()
                 << "] Received tuple with ordering timestamp: "
                 << tuple.ordering_timestamp
                 << ", WindFlow timestamp: " << context.getCurrentTimestamp()
                 << ", containing observation: " << tuple.observation
                 << ", current amount of tuples cached: " << tuple_queue.size()
                 << ", current watermark: " << watermark << '\n';
        }
#endif
        switch (execution_mode) {
        case Execution_Mode_t::DETERMINISTIC:
            process_data_stream_anomalies<T>(tuple, context);
            break;
        case Execution_Mode_t::DEFAULT:
            tuple_queue.push(tuple);

            while (!tuple_queue.empty()
                   && tuple_queue.top().ordering_timestamp <= watermark) {
                process_data_stream_anomalies<T>(tuple_queue.top(), context);
                tuple_queue.pop();
            }
            break;
        default:
            cerr << "[ANOMALY SCORER] Error: unknown execution mode\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
};

void process_sliding_window_anomalies(const ObservationResultTuple &tuple,
                                      RuntimeContext &              context)
{
#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[ANOMALY SCORER " << context.getReplicaIndex()
             << "] Received tuple with containing observation: "
             << tuple.observation << '\n';
    }
#endif
    assert(context.getLocalStorage().isContained("data"));
    auto &data =
        context.getLocalStorage().get<SlidingWindowStreamAnomalyScorerData>(
            "data");
    auto &sliding_window = data.sliding_window_map[tuple.id];

    sliding_window.push_back(tuple.score);
    if (sliding_window.size() > data.window_length) {
        sliding_window.pop_front();
    }

    double score_sum = 0;
    for (const double score : sliding_window) {
        score_sum += score;
    }
#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[ANOMALY SCORER " << context.getReplicaIndex()
             << "] Sending out tuple with observation: " << tuple.observation
             << ", score sum: " << score_sum
             << ", individual score: " << tuple.score << '\n';
    }
#endif
    const unsigned long next_ordering_timestamp =
        data.execution_mode == Execution_Mode_t::DEFAULT
            ? context.getLastWatermark()
            : tuple.ordering_timestamp;

    data.shipper->push({tuple.id, score_sum, next_ordering_timestamp,
                        tuple.parent_execution_timestamp, tuple.observation,
                        tuple.score});
}

class SlidingWindowStreamAnomalyScorerFunctor
{
    Execution_Mode_t execution_mode;
    size_t           window_length;

public:
    SlidingWindowStreamAnomalyScorerFunctor(Execution_Mode_t e, size_t length = 10):
                                            execution_mode {e},
                                            window_length {length} {}

    void operator()(const ObservationResultTuple &tuple,
                    Shipper<AnomalyResultTuple> & shipper,
                    RuntimeContext &              context)
    {
        const unsigned long watermark = context.getLastWatermark();
        assert(tuple.ordering_timestamp >= watermark);
        auto &storage = context.getLocalStorage();
        if (!storage.isContained("data")) {
            auto &data =
                storage.get<SlidingWindowStreamAnomalyScorerData>("data");
            data.execution_mode = execution_mode;
            data.window_length  = window_length;
            data.shipper        = &shipper;
        }
        auto &tuple_queue =
            storage.get<SlidingWindowStreamAnomalyScorerData>("data")
                .tuple_queue;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ANOMALY SCORER " << context.getReplicaIndex()
                 << "] Received tuple with ordering timestamp: "
                 << tuple.ordering_timestamp
                 << ", WindFlow timestamp: " << context.getCurrentTimestamp()
                 << ", containing observation: " << tuple.observation
                 << ", current amount of tuples cached: " << tuple_queue.size()
                 << ", current watermark: " << watermark << '\n';
        }
#endif
        switch (execution_mode) {
        case Execution_Mode_t::DETERMINISTIC:
            process_sliding_window_anomalies(tuple, context);
            break;
        case Execution_Mode_t::DEFAULT:
            tuple_queue.push(tuple);

            while (!tuple_queue.empty()
                   && tuple_queue.top().ordering_timestamp <= watermark) {
                assert(tuple_queue.top().ordering_timestamp == watermark);
                process_sliding_window_anomalies(tuple_queue.top(), context);
                tuple_queue.pop();
            }
            break;
        default:
            cerr << "[ANOMALY SCORER] Error: unknown execution mode\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
};

template<typename T>
struct TupleWrapper
{
    T      tuple;
    double score;

    TupleWrapper(const T &tuple, double score):
                 tuple {tuple},
                 score {score} {}

    int compare_to(const TupleWrapper &other) const
    {
        if (score == other.score) {
            return 0;
        }
        else if (score > other.score) {
            return 1;
        }
        else {
            return -1;
        }
    }
};

template<typename T>
static inline void tuple_swap(vector<TupleWrapper<T>> &tuple_wrapper_list, size_t left, size_t right)
{
    assert(left <= right);
    assert(left < tuple_wrapper_list.size());
    assert(right < tuple_wrapper_list.size());

    if (left != right) {
        const auto tmp            = move(tuple_wrapper_list[left]);
        tuple_wrapper_list[left]  = move(tuple_wrapper_list[right]);
        tuple_wrapper_list[right] = move(tmp);
    }
}

template<typename T>
static inline size_t
partition_single_side(vector<TupleWrapper<T>> &tuple_wrapper_list, size_t left, size_t right)
{
    assert(!tuple_wrapper_list.empty());
    assert(left < right);
    assert(left < tuple_wrapper_list.size());
    assert(right < tuple_wrapper_list.size());

    const auto &pivot = tuple_wrapper_list[right];
    size_t      bar   = left;

    for (size_t i = left; i < right; ++i) {
        if (tuple_wrapper_list[i].compare_to(pivot) < 0) {
            tuple_swap(tuple_wrapper_list, bar, i);
            ++bar;
        }
    }
    tuple_swap(tuple_wrapper_list, bar, right);
    return bar;
}

template<typename T>
static inline const TupleWrapper<T> &
bfprt_wrapper(vector<TupleWrapper<T>> &tuple_wrapper_list, size_t i, size_t left, size_t right)
{
    assert(!tuple_wrapper_list.empty());
    assert(left <= right);
    assert(left <= i);
    assert(i <= right);
    assert(right < tuple_wrapper_list.size());

    if (left == right) {
        return tuple_wrapper_list[right];
    }

    const size_t p = partition_single_side(tuple_wrapper_list, left, right);

    if (p == i) {
        return tuple_wrapper_list[p];
    }
    else if (p < i) {
        return bfprt_wrapper(tuple_wrapper_list, i, p + 1, right);
    }
    else {
        assert(p >= 1);
        return bfprt_wrapper(tuple_wrapper_list, i, left, p - 1);
    }
}

static inline AnomalyResultTuple bfprt(vector<AnomalyResultTuple> &tuple_list, size_t i)
{
    assert(!tuple_list.empty());
    assert(i < tuple_list.size());

    vector<TupleWrapper<AnomalyResultTuple>> tuple_wrapper_list;
    for (const auto &tuple : tuple_list) {
        tuple_wrapper_list.emplace_back(tuple, tuple.individual_score);
    }

    const auto &median_tuple =
        bfprt_wrapper(tuple_wrapper_list, i, 0, tuple_wrapper_list.size() - 1)
            .tuple;
    tuple_list.clear();

    for (const auto &wrapper : tuple_wrapper_list) {
        tuple_list.push_back(wrapper.tuple);
    }

    return median_tuple;
}

static inline const vector<AnomalyResultTuple> &identify_abnormal_streams(vector<AnomalyResultTuple> &stream_list)
{
    const size_t median_idx {stream_list.size() / 2};
    bfprt(stream_list, median_idx);
    return stream_list;
}

void process_alerts(const AnomalyResultTuple &tuple, RuntimeContext &context)
{
    assert(context.getLocalStorage().isContained("data"));
    auto &data = context.getLocalStorage().get<AlertTriggererData>("data");
    assert(tuple.ordering_timestamp >= data.previous_ordering_timestamp);
#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
             << "] Processing tuple with anomaly score: "
             << tuple.anomaly_score
             << ", individual score: " << tuple.individual_score
             << ", ordering timestamp: " << tuple.ordering_timestamp
             << ", observation: " << tuple.observation << '\n';
    }
#endif
    assert(tuple.ordering_timestamp >= data.previous_ordering_timestamp);

    if (tuple.ordering_timestamp > data.previous_ordering_timestamp) {
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                 << "] Computing abnormalities "
                    "over a stream of size "
                 << data.stream_list.size() << '\n';
        }
#endif
        if (!data.stream_list.empty()) {
            const auto abnormal_streams =
                identify_abnormal_streams(data.stream_list);
            const size_t median_idx = data.stream_list.size() / 2;
            const double min_score  = abnormal_streams[0].anomaly_score;

            assert(median_idx < abnormal_streams.size());
            const double median_score =
                abnormal_streams[median_idx].anomaly_score;
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                     << "] Median index: " << median_idx
                     << ", minimum score: " << min_score
                     << ", median score: " << median_score
                     << ", number of abnormal streams: "
                     << abnormal_streams.size() << '\n';
            }
#endif
            for (size_t i = 0; i < abnormal_streams.size(); ++i) {
                const auto & stream_profile = abnormal_streams[i];
                const double stream_score   = stream_profile.anomaly_score;
                const double cur_data_inst_score =
                    stream_profile.anomaly_score;
                const bool is_abnormal =
                    stream_score > 2 * median_score - min_score
                    && stream_score > min_score + 2 * data.dupper
                    && cur_data_inst_score
                           > 0.1 + data.min_data_instance_score;

                if (is_abnormal) {
#ifndef NDEBUG
                    {
                        lock_guard lock {print_mutex};
                        clog << "[ALERT TRIGGERER "
                             << context.getReplicaIndex()
                             << "] Sending out tuple with stream ID: "
                             << stream_profile.id
                             << ", stream score: " << stream_score
                             << ", stream profile timestamp: "
                             << stream_profile.ordering_timestamp
                             << ", is_abnormal: "
                             << (is_abnormal ? "true" : "false")
                             << ", with observation (" << tuple.observation
                             << ")\n";
                    }
#endif
                    data.shipper->push({stream_profile.id, stream_score,
                                        data.parent_execution_timestamp,
                                        is_abnormal,
                                        stream_profile.observation});
                }
            }
            data.stream_list.clear();
            data.min_data_instance_score = numeric_limits<double>::max();
            data.max_data_instance_score = 0.0;
        }
        data.previous_ordering_timestamp = tuple.ordering_timestamp;
        data.parent_execution_timestamp  = tuple.parent_execution_timestamp;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                 << "] Previous timestamp is now: "
                 << data.previous_ordering_timestamp << '\n';
        }
#endif
    }

    if (tuple.individual_score > data.max_data_instance_score) {
        data.max_data_instance_score = tuple.individual_score;
    }
    if (tuple.individual_score < data.min_data_instance_score) {
        data.min_data_instance_score = tuple.individual_score;
    }
    data.stream_list.push_back(tuple);
}

class AlertTriggererFunctor
{
    Execution_Mode_t execution_mode;

public:
    AlertTriggererFunctor(Execution_Mode_t e) : execution_mode {e} {}

    void operator()(const AnomalyResultTuple &          tuple,
                    Shipper<AlertTriggererResultTuple> &shipper,
                    RuntimeContext &                    context)
    {
        const unsigned long watermark = context.getLastWatermark();
        assert(tuple.ordering_timestamp >= watermark);
        auto &storage = context.getLocalStorage();
        if (!storage.isContained("data")) {
            auto &data          = storage.get<AlertTriggererData>("data");
            data.execution_mode = execution_mode;
            data.shipper        = &shipper;
        }
        auto &tuple_queue =
            storage.get<AlertTriggererData>("data").tuple_queue;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                 << "] Received tuple with ordering timestamp: "
                 << tuple.ordering_timestamp
                 << ", WindFlow timestamp: " << context.getCurrentTimestamp()
                 << ", current amount of tuples cached: " << tuple_queue.size()
                 << ", current watermark: " << watermark << '\n';
        }
#endif
        switch (execution_mode) {
        case Execution_Mode_t::DETERMINISTIC:
            process_alerts(tuple, context);
            break;
        case Execution_Mode_t::DEFAULT:
            tuple_queue.push(tuple);

            while (!tuple_queue.empty()
                   && tuple_queue.top().ordering_timestamp <= watermark) {
                process_alerts(tuple_queue.top(), context);
                tuple_queue.pop();
            }
            break;
        default:
            cerr << "[ALERT TRIGGERER] Error: unknown execution mode\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
};

void process_top_k_alerts(const AnomalyResultTuple &tuple, RuntimeContext &context)
{
    assert(context.getLocalStorage().isContained("data"));
    auto &data = context.getLocalStorage().get<TopKAlertTriggererData>("data");
    assert(tuple.ordering_timestamp >= data.previous_ordering_timestamp);

#ifndef NDEBUG
    {
        lock_guard lock {print_mutex};
        clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
             << "] Processing tuple with anomaly score: "
             << tuple.anomaly_score
             << ", individual score: " << tuple.individual_score
             << ", ordering timestamp: " << tuple.ordering_timestamp
             << ", observation: " << tuple.observation
             << ", current previous ordering timestamp: "
             << data.previous_ordering_timestamp << '\n';
    }
#endif

    if (tuple.ordering_timestamp > data.previous_ordering_timestamp) {
        sort(data.stream_list.begin(), data.stream_list.end());
        const size_t actual_k = data.stream_list.size() < data.k
                                    ? data.stream_list.size()
                                    : data.k;
        for (size_t i = 0; i < data.stream_list.size(); ++i) {
            auto &     tuple       = data.stream_list[i];
            const bool is_abnormal = i >= data.stream_list.size() - actual_k;
            AlertTriggererResultTuple result {tuple.id, tuple.anomaly_score,
                                              data.parent_execution_timestamp,
                                              is_abnormal, tuple.observation};
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                     << "] Sending tuple with observation: "
                     << result.observation << '\n';
            }
#endif
            data.shipper->push(move(result));
        }
        data.previous_ordering_timestamp = tuple.ordering_timestamp;
        data.parent_execution_timestamp  = tuple.parent_execution_timestamp;
        data.stream_list.clear();
    }
    data.stream_list.push_back(tuple);
}

class TopKAlertTriggererFunctor
{
    size_t           k;
    Execution_Mode_t execution_mode;

public:
    TopKAlertTriggererFunctor(Execution_Mode_t e, size_t k = 3):
                              k {k},
                              execution_mode {e} {}

    void operator()(const AnomalyResultTuple &          tuple,
                    Shipper<AlertTriggererResultTuple> &shipper,
                    RuntimeContext &                    context)
    {
        const unsigned long watermark = context.getLastWatermark();
        assert(tuple.ordering_timestamp >= watermark);
        auto &storage = context.getLocalStorage();
        if (!storage.isContained("data")) {
            auto &data          = storage.get<TopKAlertTriggererData>("data");
            data.execution_mode = execution_mode;
            data.k              = k;
            data.shipper        = &shipper;
        }
        auto &tuple_queue =
            storage.get<TopKAlertTriggererData>("data").tuple_queue;
#ifndef NDEBUG
        {
            lock_guard lock {print_mutex};
            clog << "[ALERT TRIGGERER " << context.getReplicaIndex()
                 << "] Received tuple with id " << tuple.id
                 << ", ordering timestamp: " << tuple.ordering_timestamp
                 << ", WindFlow timestamp: " << context.getCurrentTimestamp()
                 << ", current amount of tuples cached: " << tuple_queue.size()
                 << ", current watermark: " << watermark << '\n';
        }
#endif
        switch (execution_mode) {
        case Execution_Mode_t::DETERMINISTIC:
            process_top_k_alerts(tuple, context);
            break;
        case Execution_Mode_t::DEFAULT:
            tuple_queue.push(tuple);

            while (!tuple_queue.empty()
                   && tuple_queue.top().ordering_timestamp <= watermark) {
                process_top_k_alerts(tuple_queue.top(), context);
                tuple_queue.pop();
            }
            break;
        default:
            cerr << "[ALERT TRIGGERER] Error: unknown execution mode\n";
            exit(EXIT_FAILURE);
            break;
        }
    }
};

class SinkFunctor
{
    vector<unsigned long> latency_samples;
    unsigned long         tuples_received    = 0;
    unsigned long         last_sampling_time = current_time();
    unsigned long         last_arrival_time  = last_sampling_time;
    unsigned              sampling_rate;

    bool is_time_to_sample(unsigned long arrival_time)
    {
        if (sampling_rate == 0) {
            return true;
        }
        const unsigned long time_since_last_sampling =
            difference(arrival_time, last_sampling_time);
        const unsigned long time_between_samples =
            (1.0 / sampling_rate) * timeunit_scale_factor;
        return time_since_last_sampling >= time_between_samples;
    }

public:
    SinkFunctor(unsigned rate): sampling_rate {rate} {}

    void operator()(optional<AlertTriggererResultTuple> &input,
                    RuntimeContext &                     context)
    {
        DO_NOT_WARN_IF_UNUSED(context);

        if (input) {
            const unsigned long arrival_time = current_time();
            const unsigned long latency = difference(arrival_time, input->parent_execution_timestamp) / 1e03;
            ++tuples_received;
            last_arrival_time = arrival_time;
            if (is_time_to_sample(arrival_time)) {
                latency_samples.push_back(latency);
                last_sampling_time = arrival_time;
            }
#ifndef NDEBUG
            {
                lock_guard lock {print_mutex};
                clog << "[SINK " << context.getReplicaIndex()
                     << "] anomaly score: " << input->anomaly_score
                     << " is_abnormal: "
                     << (input->is_abnormal ? "true" : "false")
                     << ", containing observation: " << input->observation
                     << " arrival time: " << arrival_time
                     << " parent execution ts: "
                     << input->parent_execution_timestamp
                     << " latency: " << latency << ' ' << timeunit_string
                     << "s\n";
            }
#endif
        }
        else {
            global_received_tuples.fetch_add(tuples_received);
            global_latency_metric.merge(latency_samples);
        }
    }
};

static MultiPipe &get_source_pipe(const Parameters &parameters,
                                  PipeGraph &graph)
{
    const string name = parameters.parser_type;
    if (name == "alibaba") {
        SourceFunctor<MachineMetadata, parse_alibaba_trace> source_functor {
            parameters.duration, parameters.tuple_rate,
            parameters.execution_mode, parameters.input_file};

        const auto source =
            Source_Builder {source_functor}
                .withParallelism(parameters.parallelism[source_id])
                .withName("source")
                .build();
        return graph.add_source(source);
    }
    else if (name == "google") {
        SourceFunctor<MachineMetadata, parse_google_trace> source_functor {
            parameters.duration, parameters.tuple_rate,
            parameters.execution_mode, parameters.input_file};

        const auto source =
            Source_Builder {source_functor}
                .withParallelism(parameters.parallelism[source_id])
                .withName("source")
                .build();
        return graph.add_source(source);
    }
    else {
        cerr << "Error while building graph: unknown data parser type\n";
        exit(EXIT_FAILURE);
    }
}

static MultiPipe &get_anomaly_scorer_pipe(const Parameters &parameters,
                                          MultiPipe &pipe)
{
    const string name         = parameters.anomaly_scorer_type;
    const bool   use_chaining = parameters.use_chaining;

    if (name == "data-stream" || name == "data_stream") {
        DataStreamAnomalyScorerFunctor<MachineMetadata>
                   anomaly_scorer_functor {parameters.execution_mode};
        const auto anomaly_scorer_node =
            FlatMap_Builder {anomaly_scorer_functor}
                .withParallelism(parameters.parallelism[anomaly_scorer_id])
                .withName("anomaly scorer")
                .withKeyBy([](const ObservationResultTuple &tuple) -> string {
                    return tuple.id;
                })
                .withClosingFunction(
                    function<void(RuntimeContext &)> {process_last_tuples<
                        DataStreamAnomalyScorerData<MachineMetadata>,
                        ObservationResultTuple,
                        process_data_stream_anomalies<MachineMetadata>>})
                .build();
        return use_chaining ? pipe.chain(anomaly_scorer_node)
                            : pipe.add(anomaly_scorer_node);
    }
    else if (name == "sliding-window" || name == "sliding_window") {
        SlidingWindowStreamAnomalyScorerFunctor anomaly_scorer_functor {
            parameters.execution_mode};
        const auto anomaly_scorer_node =
            FlatMap_Builder {anomaly_scorer_functor}
                .withParallelism(parameters.parallelism[anomaly_scorer_id])
                .withName("anomaly scorer")
                .withKeyBy([](const ObservationResultTuple &tuple) -> string {
                    return tuple.id;
                })
                .withClosingFunction(function<void(RuntimeContext &)> {
                    process_last_tuples<SlidingWindowStreamAnomalyScorerData,
                                        ObservationResultTuple,
                                        process_sliding_window_anomalies>})
                .build();
        return use_chaining ? pipe.chain(anomaly_scorer_node)
                            : pipe.add(anomaly_scorer_node);
    }
    else {
        cerr << "Error while building graph: unknown Anomaly Scorer type: "
             << name << '\n';
        exit(EXIT_FAILURE);
    }
}

static MultiPipe &get_alert_triggerer_pipe(const Parameters &parameters,
                                           MultiPipe &pipe)
{
    const string name         = parameters.alert_triggerer_type;
    const bool   use_chaining = parameters.use_chaining;

    if (name == "top-k" || name == "top_k") {
        TopKAlertTriggererFunctor alert_triggerer_functor {
            parameters.execution_mode};
        const auto alert_triggerer_node =
            FlatMap_Builder {alert_triggerer_functor}
                .withParallelism(parameters.parallelism[alert_triggerer_id])
                .withName("alert triggerer")
                .withClosingFunction(function<void(RuntimeContext &)> {
                    process_last_tuples<TopKAlertTriggererData,
                                        AnomalyResultTuple,
                                        process_top_k_alerts>})
                .build();
        return use_chaining ? pipe.chain(alert_triggerer_node)
                            : pipe.add(alert_triggerer_node);
    }
    else if (name == "default") {
        AlertTriggererFunctor alert_triggerer_functor {
            parameters.execution_mode};
        const auto alert_triggerer_node =
            FlatMap_Builder {alert_triggerer_functor}
                .withParallelism(parameters.parallelism[alert_triggerer_id])
                .withName("alert triggerer")
                .withClosingFunction(function<void(RuntimeContext &)> {
                    process_last_tuples<AlertTriggererData, AnomalyResultTuple,
                                        process_alerts>})
                .build();

        return use_chaining ? pipe.chain(alert_triggerer_node)
                            : pipe.add(alert_triggerer_node);
    }
    else {
        cerr << "Error while building graph: unknown Alert Triggerer type: "
             << name << '\n';
        exit(EXIT_FAILURE);
    }
}

static inline PipeGraph &build_graph(const Parameters &parameters,
                                     PipeGraph &graph)
{
    auto &source_pipe = get_source_pipe(parameters, graph);

    ObservationScorerFunctor<MachineMetadataScorer> observer_functor {
        parameters.execution_mode};
    const auto observer_scorer_node =
        FlatMap_Builder {observer_functor}
            .withParallelism(parameters.parallelism[observer_id])
            .withName("observation scorer")
            .withClosingFunction(
                function<void(RuntimeContext &)> {process_last_tuples<
                    ObservationScorerData<MachineMetadataScorer>, SourceTuple,
                    process_observations<MachineMetadataScorer>>})
            .build();

    auto &observation_scorer_pipe =
        parameters.use_chaining ? source_pipe.chain(observer_scorer_node)
                                : source_pipe.add(observer_scorer_node);

    auto &anomaly_scorer_pipe =
        get_anomaly_scorer_pipe(parameters, observation_scorer_pipe);

    auto &alert_triggerer_pipe =
        get_alert_triggerer_pipe(parameters, anomaly_scorer_pipe);

    SinkFunctor sink_functor {parameters.sampling_rate};
    const auto  sink = Sink_Builder {sink_functor}
                          .withParallelism(parameters.parallelism[sink_id])
                          .withName("sink")
                          .build();

    if (parameters.use_chaining) {
        alert_triggerer_pipe.chain_sink(sink);
    }
    else {
        alert_triggerer_pipe.add_sink(sink);
    }
    return graph;
}

#if defined(NDEBUG) && !defined(PROFILE)
static inline nlohmann::ordered_json
add_mo_stats(const nlohmann::ordered_json &json_stats,
             const Parameters &parameters)
{
    auto updated_json_stats = json_stats;

    updated_json_stats["anomaly scorer"]  = parameters.anomaly_scorer_type;
    updated_json_stats["alert triggerer"] = parameters.alert_triggerer_type;
    return updated_json_stats;
}
#endif

int main(int argc, char *argv[])
{
    Parameters parameters;
    parse_args(argc, argv, parameters);
    validate_args(parameters);
    PipeGraph graph {"MachineOutlier", parameters.execution_mode,
                     parameters.time_policy};
    build_graph(parameters, graph);
    print_initial_parameters(parameters);

    const unsigned long start_time = current_time();
    graph.run();
    const unsigned long elapsed_time = difference(current_time(), start_time);

// #if defined(NDEBUG) && !defined(PROFILE)
//     const double throughput =
//         elapsed_time > 0
//             ? (global_sent_tuples.load() / static_cast<double>(elapsed_time))
//             : global_sent_tuples.load();

//     const double service_time = 1 / throughput;

//     const auto latency_stats =
//         add_mo_stats(get_distribution_stats(global_latency_metric, parameters,
//                                             global_received_tuples),
//                      parameters);
//     serialize_json(latency_stats, "mo-latency",
//                    parameters.metric_output_directory);

//     const auto throughput_stats = add_mo_stats(
//         get_single_value_stats(throughput, "throughput", parameters,
//                                global_sent_tuples.load()),
//         parameters);
//     serialize_json(throughput_stats, "mo-throughput",
//                    parameters.metric_output_directory);

//     const auto service_time_stats = add_mo_stats(
//         get_single_value_stats(service_time, "service time", parameters,
//                                global_sent_tuples.load()),
//         parameters);
//     serialize_json(service_time_stats, "mo-service-time",
//                    parameters.metric_output_directory);
// #endif

    const double elapsed_time_in_seconds =
        elapsed_time / static_cast<double>(timeunit_scale_factor);
    unsigned long sent_tuples = global_sent_tuples;
    const double throughput =
        elapsed_time > 0 ? sent_tuples / static_cast<double>(elapsed_time)
                         : sent_tuples;
    const double throughput_in_seconds = throughput * timeunit_scale_factor;
    
    const double average_latency =
        accumulate(global_latency_metric.begin(), global_latency_metric.end(),
                   0.0)
        / (!global_latency_metric.empty() ? global_latency_metric.size()
                                          : 1.0);
    const auto latency_stats = get_distribution_stats(
        global_latency_metric, parameters, global_received_tuples);
    serialize_json(latency_stats, "metric_latency",
                   parameters.metric_output_directory);

    cout << "Exiting" << endl;
    cout << "Measured throughput: " << (int) throughput_in_seconds << " tuples/second" << endl;
    cout << "Dumping metrics" << endl;
    return 0;
}
