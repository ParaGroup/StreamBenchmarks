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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Implementation of the On-demand Time-decaying Bloom Filter. It is similar to a
 * time decaying bloom filter, except for the fact that only the counters that are
 * going to be used are updated, instead of all of them periodically.
 * <p/>
 * TODO: replace the 2d array of buckets and introduce bitwise operations
 * <p/>
 * Reference:
 * Dusi, Maurizio, et al. "Blockmon: Flexible and high-performance big data stream
 * analytics platform and its use cases." NEC Technical Journal 7.2 (2012): 103.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ODTDBloomFilter {
    private static final MurmurHash hasher = new MurmurHash();

    private int bucketsPerWord;
    private int hashCount;
    private int numBuckets;
    private double[][] buckets;
    private long[] timers;
    private double beta;

    /**
     * Constructor with default number of buckets per word (16).
     *
     * @param numElements       Expected number of distinct elements
     * @param bucketsPerElement The number of buckets per element, used to calculate number of hashes (k)
     * @param beta              The normalized smoothing coeficient, i.e. measured rate decay in the unit time
     */
    public ODTDBloomFilter(int numElements, int bucketsPerElement, double beta) {
        this(numElements, bucketsPerElement, beta, 16);
    }

    /**
     * Creates a new filter with k hash functions accordingly with {@link BloomCalculations#computeBestK(int)}.
     * Each bucket will have bucketsPerWord counters, but since this implementation
     * uses a 2D array there's no limitation in the counter value, unlike CountingBloomFilter.
     *
     * @param numElements       Expected number of distinct elements
     * @param bucketsPerElement The number of buckets per element, used to calculate number of hashes (k)
     * @param beta              The normalized smoothing coeficient, i.e. measured rate decay in the unit time
     * @param bucketsPerWord    Number of counters within a bucket (bin)
     */
    public ODTDBloomFilter(int numElements, int bucketsPerElement, double beta, int bucketsPerWord) {
        this.beta = beta;
        this.bucketsPerWord = bucketsPerWord;

        hashCount = BloomCalculations.computeBestK(bucketsPerElement);
        numBuckets = (numElements * bucketsPerElement + 20) / bucketsPerWord;

        buckets = new double[numBuckets][bucketsPerWord];
        timers = new long[numBuckets];
    }

    // murmur is faster than a sha-based approach and provides as-good collision
    // resistance.  the combinatorial generation approach described in
    // http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    public static int[] getHashBuckets(String item, int hashCount, int max) {
        byte[] b;
        try {
            b = item.getBytes("UTF-16");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        int[] result = new int[hashCount];
        int hash1 = hasher.hash(b, b.length, 0);
        int hash2 = hasher.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    private static long time() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Add the item to the data structure with quantity's value one.
     *
     * @param item The item to be added
     */
    public void add(String item) {
        add(item, 1, time());
    }

    public void add(String item, int q) {
        add(item, q, time());
    }

    /**
     * Add an item to the data structure.
     *
     * @param item      The item to be added
     * @param q         The quantity for the key
     * @param timestamp The time of insertion in seconds (epoch time)
     */
    public void add(String item, int q, long timestamp) {
        assert item != null;
        double count = estimateCount(item, timestamp) + ((double) q * Math.log(1 / beta));

        for (int bucketIndex : getHashBuckets(item)) {
            if (getBucket(bucketIndex) < count)
                setBucket(bucketIndex, count);
        }
    }

    public double estimateCount(String item) {
        return estimateCount(item, time());
    }

    /**
     * Estimate the count for an item.
     *
     * @param item The item to be estimated
     * @param time The timestamp in seconds
     * @return The counter value for the item
     */
    public double estimateCount(String item, long time) {
        double res = Double.MAX_VALUE;

        for (int bucketIndex : getHashBuckets(item)) {
            // update the counter with the smoothing coeficient
            double value = getBucket(bucketIndex) * Math.pow(beta, time - getTimer(bucketIndex));
            setBucket(bucketIndex, value);

            // update the bucket timer
            setTimer(bucketIndex, time);

            if (value < res)
                res = value;
        }
        return (res != Double.MAX_VALUE) ? res : 0;
    }

    /**
     * @return Return the total number of counters in the data structure
     */
    private int buckets() {
        return buckets.length * bucketsPerWord;
    }

    /**
     * Get a list of hashes for an item
     *
     * @param item The item for calculating the hashes
     * @return The list of hashes
     */
    private int[] getHashBuckets(String item) {
        return getHashBuckets(item, hashCount, buckets());
    }

    private double getBucket(int bucketIndex) {
        return buckets[bucketIndex / bucketsPerWord][bucketIndex % bucketsPerWord];
    }

    private void setBucket(int bucketIndex, double value) {
        buckets[bucketIndex / bucketsPerWord][bucketIndex % bucketsPerWord] = value;
    }

    private long getTimer(int bucketIndex) {
        return timers[bucketIndex / bucketsPerWord];
    }

    private void setTimer(int bucketIndex, long value) {
        timers[bucketIndex / bucketsPerWord] = value;
    }
}
