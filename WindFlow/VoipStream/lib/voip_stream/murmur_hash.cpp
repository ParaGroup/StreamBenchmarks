#include "murmur_hash.hpp"

namespace voip_stream {

int murmur_hash(const std::string &data, int seed)
{
    const auto length = data.size();
    int m = 0x5bd1e995;
    int r = 24;

    int h = seed ^ length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
        int i_4 = i << 2;
        int k = data[i_4 + 3];
        k = k << 8;
        k = k | (data[i_4 + 2] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 1] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 0] & 0xff);
        k *= m;
        k ^= (unsigned int)k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
        if (left >= 3) {
            h ^= (int) data[length - 3] << 16;
        }
        if (left >= 2) {
            h ^= (int) data[length - 2] << 8;
        }
        if (left >= 1) {
            h ^= (int) data[length - 1];
        }

        h *= m;
    }

    h ^= (unsigned int)h >> 13;
    h *= m;
    h ^= (unsigned int)h >> 15;

    return h;
}

}
