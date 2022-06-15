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

package common;

/**
 * The following calculations are taken from:
 * http://www.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * "Bloom Filters - the math"
 * <p/>
 * This class's static methods are meant to facilitate the use of the Bloom
 * Filter class by helping to choose correct values of 'buckets per element' and
 * 'number of hash functions, k'.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class BloomCalculations {
    private static final int[] optKPerBuckets =
            new int[]{1, // dummy K for 0 buckets per element
                    1, // dummy K for 1 buckets per element
                    1, 2, 3, 3, 4, 5, 5, 6, 7, 8, 8, 8, 8, 8};

    /**
     * Given the number of buckets that can be used per element, return the optimal
     * number of hash functions in order to minimize the false positive rate.
     *
     * @param bucketsPerElement
     * @return The number of hash functions that minimize the false positive rate.
     */
    public static int computeBestK(int bucketsPerElement) {
        assert bucketsPerElement >= 0;
        if (bucketsPerElement >= optKPerBuckets.length)
            return optKPerBuckets[optKPerBuckets.length - 1];
        return optKPerBuckets[bucketsPerElement];
    }
}
