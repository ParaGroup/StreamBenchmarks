#pragma once

#include <string>
#include <vector>

namespace voip_stream {

class ODTDBloomFilter {
public:

    ODTDBloomFilter(int num_elements, int buckets_per_element, double beta);
    ODTDBloomFilter(int num_elements, int buckets_per_element, double beta, int buckets_per_word);
    std::vector<int> get_hash_buckets(const std::string &item, int hash_count, int max) const;
    long time() const;
    void add(const std::string &item);
    void add(const std::string &item, int q);
    void add(const std::string &item, int q, long timestamp);
    double estimate_count(const std::string &item);
    double estimate_count(const std::string &item, long time);
    int buckets() const;
    std::vector<int> get_hash_buckets(const std::string &item) const;
    double get_bucket(int bucket_index) const;
    void set_bucket(int bucket_index, double value);
    long get_timer(int bucket_index) const;
    void set_timer(int bucket_index, long value);

private:

    double beta_;
    int buckets_per_word_;
    int hash_count_;
    int num_buckets_;
    std::vector<std::vector<double>> buckets_;
    std::vector<long> timers_;
};

}
