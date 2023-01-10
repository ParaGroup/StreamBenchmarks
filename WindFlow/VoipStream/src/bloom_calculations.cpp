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

#include<cassert>

namespace voip_stream {

static const int OPT_K_PER_BUCKETS[] = {
    1, // dummy K for 0 buckets per element
    1, // dummy K for 1 buckets per element
    1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 8, 8, 8
};

int compute_best_k(int buckets_per_element) {
    assert(buckets_per_element >= 0);
    constexpr auto length = sizeof(OPT_K_PER_BUCKETS) / sizeof(int);
    if (buckets_per_element >= (int) length)
        return OPT_K_PER_BUCKETS[length - 1];
    return OPT_K_PER_BUCKETS[buckets_per_element];
}

}
