#include "line_reader_source.hpp"
#include "log.hpp"
#include "metric_group.hpp"
#include <chrono>
#include <thread>
#include <atomic>

// global variable for throughput
extern std::atomic<unsigned long> sent_tuples;

namespace util {

std::ostream &operator <<(std::ostream &os, const LineReaderSource::Tuple &tuple)
{
    return os << tuple.ts << ' ' << *tuple.line;
}

LineReaderSource::LineReaderSource(int run_time_sec, int gen_rate, const std::string &path)
    : run_time_sec_(run_time_sec)
    , gen_rate_(gen_rate)
    , index_(0)
    , counter_(0)
{
    read_all(path);
    epoch_ = wf::current_time_nsecs();
}

bool LineReaderSource::operator ()(Tuple &tuple, wf::RuntimeContext &rc)
{
    if (gen_rate_ != 0) {
        auto start_time = wf::current_time_usecs();
        bool end = false;
        while (!end) {
            auto end_time = wf::current_time_usecs();
            end = (end_time - start_time) >= ((unsigned long) 1000000 / gen_rate_);
        }
    }

    // fetch the next item
    if (index_ == 0 || index_ >= data_.size()) {
        index_ = rc.getReplicaIndex();
    }
    const auto &line = data_.at(index_);
    index_ += rc.getParallelism();
    ++counter_;

    // fill timestamp and value
    tuple.ts = wf::current_time_nsecs();
    tuple.line = &line;

    DEBUG_LOG("source::tuple " << tuple);

    // set the epoch for the first time
    if (counter_ == 1) {
        epoch_ = tuple.ts;
    }

    if (tuple.ts - epoch_ < run_time_sec_ * 1e9) { // nanoseconds
        // call this function again
        return true;
    } else {
        DEBUG_LOG("source::finished");

        // this is the last emitted tuple so metrics are gathered
        auto rate = counter_ / ((tuple.ts - epoch_) / 1e9); // per second
        throughput_.add(rate);
        //util::metric_group.add("throughput", throughput_);
        sent_tuples.fetch_add(counter_);
        return false;
    }
}

void LineReaderSource::read_all(const std::string &path) {
    std::ifstream fs(path);
    std::string line;

    while (std::getline(fs, line)) {
        data_.push_back(line);
    }
}

}
