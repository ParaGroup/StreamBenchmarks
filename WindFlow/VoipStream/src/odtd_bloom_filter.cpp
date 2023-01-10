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

#include<voip_stream/bloom_calculations.hpp>
#include<voip_stream/murmur_hash.hpp>
#include<voip_stream/odtd_bloom_filter.hpp>
#include<cmath>
#include<limits>
#include<windflow.hpp>

namespace voip_stream {

ODTDBloomFilter::ODTDBloomFilter(int num_elements, int buckets_per_element, double beta)
    : ODTDBloomFilter(num_elements, buckets_per_element, beta, 16)
{}

ODTDBloomFilter::ODTDBloomFilter(int num_elements, int buckets_per_element, double beta, int buckets_per_word)
    : beta_(beta)
    , buckets_per_word_(buckets_per_word)
    , hash_count_(compute_best_k(buckets_per_element))
    , num_buckets_((num_elements * buckets_per_element + 20) / buckets_per_word)
    , buckets_(num_buckets_, std::vector<double>(buckets_per_word))
    , timers_(num_buckets_)
{}

std::vector<int> ODTDBloomFilter::get_hash_buckets(const std::string &item, int hash_count, int max) const
{
    // XXX it was UTF-16...
    std::vector<int> result(hash_count);
    int hash1 = murmur_hash(item, 0);
    int hash2 = murmur_hash(item, hash1);
    for (int i = 0; i < hash_count; i++) {
        result[i] = std::abs((hash1 + i * hash2) % max);
    }
    return result;
}

long ODTDBloomFilter::time() const
{
    return wf::current_time_nsecs() / 1e9;
}

void ODTDBloomFilter::add(const std::string &item)
{
    add(item, 1, time());
}

void ODTDBloomFilter::add(const std::string &item, int q)
{
    add(item, q, time());
}

void ODTDBloomFilter::add(const std::string &item, int q, long timestamp)
{
    double count = estimate_count(item, timestamp) + ((double) q * std::log(1 / beta_));

    for (int bucket_index : get_hash_buckets(item)) {
        if (get_bucket(bucket_index) < count)
            set_bucket(bucket_index, count);
    }
}

double ODTDBloomFilter::estimate_count(const std::string &item)
{
    return estimate_count(item, time());
}

double ODTDBloomFilter::estimate_count(const std::string &item, long time)
{
    double res = std::numeric_limits<double>::max();

    for (int bucket_index : get_hash_buckets(item)) {
        // update the counter with the smoothing coeficient
        double value = get_bucket(bucket_index) * std::pow(beta_, time - get_timer(bucket_index));
        set_bucket(bucket_index, value);

        // update the bucket timer
        set_timer(bucket_index, time);

        if (value < res)
            res = value;
    }
    return (res != std::numeric_limits<double>::max()) ? res : 0;
}

int ODTDBloomFilter::buckets() const
{
    return buckets_.size() * buckets_per_word_;
}

std::vector<int> ODTDBloomFilter::get_hash_buckets(const std::string &item) const
{
    return get_hash_buckets(item, hash_count_, buckets());
}

double ODTDBloomFilter::get_bucket(int bucket_index) const
{
    return buckets_[bucket_index / buckets_per_word_][bucket_index % buckets_per_word_];
}

void ODTDBloomFilter::set_bucket(int bucket_index, double value)
{
    buckets_[bucket_index / buckets_per_word_][bucket_index % buckets_per_word_] = value;
}

long ODTDBloomFilter::get_timer(int bucket_index) const
{
    return timers_[bucket_index / buckets_per_word_];
}

void ODTDBloomFilter::set_timer(int bucket_index, long value)
{
    timers_[bucket_index / buckets_per_word_] = value;
}

}
