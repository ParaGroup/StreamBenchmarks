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
