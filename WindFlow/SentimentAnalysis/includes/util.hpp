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

#ifndef UTIL_HPP
#define UTIL_HPP

#include<algorithm>
#include<cstddef>
#include<cstdint>
#include<cstdlib>
#include<ctime>
#include<dirent.h>
#include<iostream>
#include<mutex>
#include<nlohmann/json.hpp>
#include<string>
#include<string_view>
#include<sys/stat.h>
#include<sys/time.h>
#include<sys/types.h>
#include<vector>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wsign-compare"
#if defined(__clang__) || !defined(__GNUC__)
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#endif
#pragma GCC diagnostic ignored "-Woverloaded-virtual"
#endif

#include<windflow.hpp>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

#define DO_NOT_WARN_IF_UNUSED(x) ((void) (x))

static constexpr auto current_time = wf::current_time_nsecs;

static const auto timeunit_string =
    current_time == wf::current_time_usecs   ? "microsecond"
    : current_time == wf::current_time_nsecs ? "nanosecond"
                                             : "time unit";

static const unsigned long timeunit_scale_factor =
    current_time == wf::current_time_usecs   ? 1000000
    : current_time == wf::current_time_nsecs ? 1000000000
                                             : 1;

static uint64_t current_time_secs()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return t.tv_sec;
}

// Return difference between a and b, accounting for unsigned arithmetic wraparound
static unsigned long difference(unsigned long a, unsigned long b)
{
    return std::max(a, b) - std::min(a, b);
}

// suspend execution for an amount of time units specified by duration.
static inline void busy_wait(unsigned long duration)
{
    const unsigned long start_time = current_time();
    unsigned long       now        = start_time;
    while (now - start_time < duration) {
        now = current_time();
    }
}

// return a std::vector of std::string_views, obtained from splitting the original string_view. by the delim character.
static inline std::vector<std::string_view>
string_split(const std::string_view &s, char delim)
{
    const auto is_delim   = [=](char c) { return c == delim; };
    auto       word_begin = std::find_if_not(s.begin(), s.end(), is_delim);
    std::vector<std::string_view> words;

    while (word_begin < s.end()) {
        const auto word_end = std::find_if(word_begin + 1, s.end(), is_delim);
        words.emplace_back(word_begin, word_end - word_begin);
        word_begin = std::find_if_not(word_end, s.end(), is_delim);
    }
    return words;
}

static inline std::vector<size_t>
get_nums_split_by_commas(const char *degrees)
{
    std::vector<std::size_t> parallelism_degrees;
    for (const auto &s : string_split(degrees, ',')) {
        parallelism_degrees.push_back(std::atoi(s.data()));
    }
    return parallelism_degrees;
}

static inline std::string get_datetime_string()
{
    const auto  current_date = std::time(nullptr);
    std::string date_string  = std::asctime(localtime(&current_date));
    if (!date_string.empty()) {
        date_string.pop_back(); // needed to remove trailing newline
    }
    return date_string;
}

static inline wf::Execution_Mode_t get_execution_mode_from_string(const std::string &s)
{
    if (s == "default") {
        return wf::Execution_Mode_t::DEFAULT;
    }
    else if (s == "deterministic") {
        return wf::Execution_Mode_t::DETERMINISTIC;
    }
    else if (s == "probabilistic") {
        return wf::Execution_Mode_t::PROBABILISTIC;
    } 
    else {
        std::cerr
            << "get_execution_mode_from_string:  error, invalid string\n";
        std::exit(EXIT_FAILURE);
    }
    return wf::Execution_Mode_t::DEFAULT; // Make the compiler happy
}

static inline wf::Time_Policy_t get_time_policy_from_string(const std::string &s)
{
    if (s == "ingress_time") {
        return wf::Time_Policy_t::INGRESS_TIME;
    }
    else if (s == "event_time") {
        return wf::Time_Policy_t::EVENT_TIME;
    }
    else {
        std::cerr << "get_time_policy_from_string:  error, invalid string\n";
        std::exit(EXIT_FAILURE);
    }
    return wf::Time_Policy_t::INGRESS_TIME; // Make the compiler happy
}

static inline const char * get_string_from_execution_mode(wf::Execution_Mode_t e)
{
    switch (e) {
    case wf::Execution_Mode_t::DEFAULT:
        return "default";
    case wf::Execution_Mode_t::DETERMINISTIC:
        return "deterministic";
    case wf::Execution_Mode_t::PROBABILISTIC:
        return "probabilistic";
    default:
        std::cerr << "get_string_from_execution_mode:  error, invalid "
                     "execution mode\n";
        std::exit(EXIT_FAILURE);
        break;
    }
    return ""; // Make the compiler happy
}

static inline const char *get_string_from_time_policy(wf::Time_Policy_t p)
{
    switch (p) {
    case wf::Time_Policy_t::INGRESS_TIME:
        return "ingress time";
    case wf::Time_Policy_t::EVENT_TIME:
        return "event time";
    default:
        std::cerr
            << "get_string_from_time_policy: error, invalid time policy\n";
        std::exit(EXIT_FAILURE);
        break;
    }
    return ""; // Make the compiler happy
}

static inline bool get_bool_from_string(const std::string &arg)
{
    return arg == "true"           ? true
           : arg == "false"        ? false
           : std::atoi(optarg) > 0 ? true
                                   : false;
}

template<typename T>
class Metric {
    std::vector<T> sorted_samples;
    std::string    metric_name;
    std::mutex     metric_mutex;

public:
    Metric(const std::string &name = "name") : metric_name {name} {}

    Metric &merge(const std::vector<T> &new_samples)
    {
        std::lock_guard guard {metric_mutex};
        sorted_samples.insert(sorted_samples.begin(), new_samples.begin(),
                              new_samples.end());
        sort(sorted_samples.begin(), sorted_samples.end());
        return *this;
    }

    std::size_t size() const
    {
        return sorted_samples.size();
    }

    std::size_t length() const
    {
        return sorted_samples.length();
    }

    bool empty() const
    {
        return sorted_samples.empty();
    }

    typename std::vector<T>::const_iterator begin() const
    {
        return sorted_samples.begin();
    }

    typename std::vector<T>::const_iterator end() const
    {
        return sorted_samples.end();
    }

    const char *name() const
    {
        return metric_name.c_str();
    }
};

static inline void create_directory_if_not_exists(const char *path) noexcept
{
    const auto dir = opendir(path);
    if (dir) {
        const int status = closedir(dir);
        if (status != 0) {
            std::cerr << "Error closing directory " << path << '\n';
            std::exit(EXIT_FAILURE);
        }
    }
    else {
        const int status = mkdir(path, S_IRWXU | S_IRGRP | S_IROTH);
        if (status != 0) {
            std::cerr << "Error creating directory\n";
            std::exit(EXIT_FAILURE);
        }
    }
}

template<typename Parameters>
static inline nlohmann::ordered_json get_distribution_stats(const Metric<unsigned long> &metric,
                                                            const Parameters &parameters,
                                                            unsigned long total_measurements)
{
    nlohmann::ordered_json json_stats;
    json_stats["sampled measurements"] = metric.size();
    json_stats["total measurements"]   = total_measurements;
    if (!metric.empty()) {
        const double mean =
            accumulate(metric.begin(), metric.end(), 0.0) / metric.size();
        json_stats["mean"] = mean;

        for (const auto percentile : {0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0}) {
            const auto percentile_value_position =
                metric.begin() + (metric.size() - 1) * percentile;
            const auto label =
                std::to_string(static_cast<int>(percentile * 100))
                + "th percentile";
            json_stats[label] = *percentile_value_position;
        }
    }
    else {
        json_stats["mean"] = 0;
        for (const auto percentile : {"0", "25", "50", "75", "95", "100"}) {
            const auto label  = std::string {percentile} + "th percentile";
            json_stats[label] = 0;
        }
    }
    return json_stats;
}

template<typename T, typename Parameters>
static inline nlohmann::ordered_json get_single_value_stats(T value, const std::string &name,
                                                            const Parameters &parameters,
                                                            unsigned long total_measurements)
{
    nlohmann::ordered_json json_stats;
    json_stats["total measurements"] = total_measurements;
    json_stats["mean"] = value;
    return json_stats;
}

static inline void serialize_json(const nlohmann::ordered_json &json_stats,
                                  const std::string &file_name,
                                  const std::string &output_directory)
{
    create_directory_if_not_exists(output_directory.c_str());
    std::ofstream fs {"metric_latency.json"};
    fs << json_stats.dump(4) << '\n';
}

#endif // #ifndef UTIL_HPP
