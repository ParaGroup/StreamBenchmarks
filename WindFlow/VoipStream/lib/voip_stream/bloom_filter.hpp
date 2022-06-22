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

#pragma once

#include <cmath>
#include <openssl/md5.h>
#include <string>
#include <vector>

namespace voip_stream {

template <typename T>
class BloomFilter
{
public:

    BloomFilter(double c, int n, int k)
        : bit_set_size_(std::ceil(c * n))
        , bits_per_element_(c)
        , expected_number_of_filter_elements_(n)
        , number_of_added_elements_(0)
        , k_(k)
        , num_non_zero_(0)
    {}

    BloomFilter(double false_positive_probability, int expected_number_of_elements)
        : BloomFilter(std::ceil(-(std::log(false_positive_probability) / std::log(2))) / std::log(2), // c = k / ln(2)
                      expected_number_of_elements,
                      (int) std::ceil(-(std::log(false_positive_probability) / std::log(2))))
    {}

    static std::vector<int> create_hashes(const std::string &data, int hashes)
    {
        std::vector<int> result(hashes);

        int k = 0;
        char salt = 0;
        while (k < hashes) {
            uint8_t digest[16] = {0};

            auto salted_data = data + salt++;
            MD5(reinterpret_cast<const uint8_t *>(salted_data.data()), salted_data.size(), digest);

            for (int i = 0; i < 16 / 4 && k < hashes; i++) {
                int h = 0;
                for (int j = (i * 4); j < (i * 4) + 4; j++) {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    bool operator ==(const BloomFilter &other)
    {
        if (expected_number_of_filter_elements_ != other.expected_number_of_filter_elements_) {
            return false;
        }

        if (k_ != other.k_) {
            return false;
        }

        // XXX complex condition simplified
        return bit_set_size_ == other.bit_set_size_ && bitset_ == other.bitset_;
    }

    void clear()
    {
        bitset_.clear();
        number_of_added_elements_ = 0;
        num_non_zero_ = 0;
    }

    void add(const std::string &bytes)
    {
        std::vector<int> hashes = create_hashes(bytes, k_);
        for (int hash : hashes) {
            size_t index = std::abs(hash % bit_set_size_);
            if (index >= bitset_.size() || !bitset_.at(index)) num_non_zero_++;
            if (index >= bitset_.size()) bitset_.resize(index);
            bitset_[index] = true;
        }
        number_of_added_elements_++;
    }

    int get_num_non_zero() const
    {
        return num_non_zero_;
    }

    bool membership_test(const std::string &bytes) const
    {
        std::vector<int> hashes = create_hashes(bytes, k_);
        for (int hash : hashes) {
            size_t index = std::abs(hash % bit_set_size_);
            if (index >= bitset_.size() || !bitset_.at(index)) {
                return false;
            }
        }
        return true;
    }

    int size() const
    {
        return bit_set_size_;
    }

    int count() const
    {
        return number_of_added_elements_;
    }

private:

    std::vector<bool> bitset_;
    int bit_set_size_;
    double bits_per_element_;
    int expected_number_of_filter_elements_; // expected (maximum) number of elements to be added
    int number_of_added_elements_; // number of elements actually added to the Bloom filter
    int k_; // number of hash functions
    int num_non_zero_;
};

}

namespace std {

template<typename T>
struct hash<voip_stream::BloomFilter<T>>
{
    size_t operator ()(const voip_stream::BloomFilter<T> &object) const
    {
        int hash = 7;
        hash = 61 * hash + std::hash<std::vector<bool>>()(object.bitset_);
        hash = 61 * hash + object.expected_number_of_filter_elements_;
        hash = 61 * hash + object.bit_set_size_;
        hash = 61 * hash + object.k_;
        return hash;
    }
};

}
