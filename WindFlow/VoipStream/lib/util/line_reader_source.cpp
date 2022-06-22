/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Andrea Cardaci
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
