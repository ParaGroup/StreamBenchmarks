/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
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

#ifndef TRAFFICMONITORING_RESULT_HPP
#define TRAFFICMONITORING_RESULT_HPP

#include <windflow.hpp>

using namespace std;

struct result_t {
    double speed;               // vehicle speed
    size_t key;                 // road ID corresponding to latitude and longitude coordinates of the vehicle
    uint64_t ts;

    // default constructor
    result_t(): speed(0.0), key(0) {}

    // constructor
    result_t(double _speed, size_t _key, uint64_t _id, uint64_t _ts): speed(_speed), key(_key) {}
};

#endif //TRAFFICMONITORING_RESULT_HPP
