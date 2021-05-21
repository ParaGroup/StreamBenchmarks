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

void LineReaderSource::active_delay(unsigned long waste_time) {
    auto start_time = wf::current_time_nsecs();
    bool end = false;
    while (!end) {
        auto end_time = wf::current_time_nsecs();
        end = (end_time - start_time) >= waste_time;
    }
}

void LineReaderSource::operator ()(wf::Source_Shipper<LineReaderSource::Tuple> &shipper, wf::RuntimeContext &rc)
{
		epoch_ = wf::current_time_nsecs();
    	// generation loop
    	while (wf::current_time_nsecs() - epoch_ <= run_time_sec_ * 1e9)
    	{
		    // fetch the next item
		    if (index_ == 0 || index_ >= data_.size()) {
		        index_ = rc.getReplicaIndex();
		    }    		
		    const auto &line = data_.at(index_);
		    index_ += rc.getParallelism();
		    ++counter_; 
		    // fill timestamp and value
		    Tuple tuple;
		    tuple.ts = wf::current_time_nsecs();
		    tuple.line = &line;
		    DEBUG_LOG("source::tuple " << tuple);
		    shipper.push(std::move(tuple));
		    if (gen_rate_ != 0) { // active waiting to respect the generation rate
	            long delay_nsec = (long) ((1.0d / gen_rate_) * 1e9);
	            active_delay(delay_nsec);
	        }
		}
		sent_tuples.fetch_add(counter_);
}

void LineReaderSource::read_all(const std::string &path) {
    std::ifstream fs(path);
    std::string line;

    while (std::getline(fs, line)) {
        data_.push_back(line);
    }
}

}
