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
