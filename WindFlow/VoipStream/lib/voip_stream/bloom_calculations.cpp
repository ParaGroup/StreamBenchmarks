#include <cassert>

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
