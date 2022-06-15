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

#ifndef TRAFFICMONITORING_TUPLE_HPP
#define TRAFFICMONITORING_TUPLE_HPP

#include <windflow.hpp>

using namespace std;

struct tuple_t {
    double latitude;            // vehicle latitude
    double longitude;           // vehicle longitude
    double speed;               // vehicle speed
    int direction;              // vehicle direction
    size_t key;                 // vehicle_id that identifies the vehicle (taxi or bus)
    uint64_t ts;

    // default constructor
    tuple_t() : latitude(0.0), longitude(0.0), speed(0.0), direction(0), key(0) {}

    // constructor
    tuple_t(double _latitude, double _longitude, double _speed, int _direction, size_t _key) :
        latitude(_latitude), longitude(_longitude), speed(_speed), direction(_direction), key(_key) {}
};

#endif //TRAFFICMONITORING_TUPLE_HPP
