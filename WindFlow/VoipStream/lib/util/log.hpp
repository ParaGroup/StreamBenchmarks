#pragma once

#include <mutex>
#include <sys/types.h>
#include <type_traits>

#ifdef DEBUG
#define DEBUG_LOG(info) do {                                                   \
        if (getenv("LOG_LEVEL")) {                                             \
            std::lock_guard<std::mutex> lock(util::log_mutex);                 \
            std::cout << gettid() << ' ' << info << std::endl;                 \
        }                                                                      \
    } while (false)
#else
#define DEBUG_LOG(info) do {} while (false)
#endif

namespace util {

extern std::mutex log_mutex;

}
