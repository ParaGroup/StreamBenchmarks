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

#ifndef SPIKEDETECTION_TUPLE_HPP
#define SPIKEDETECTION_TUPLE_HPP

#include<windflow.hpp>

using namespace std;

// tuple_t struct
struct tuple_t
{
    double property_value;
    double incremental_average;
    size_t key;
    // uint64_t ts;

    // Default Constructor
    tuple_t():
            property_value(0.0),
            incremental_average(0.0),
            key(0) {}

    // Constructor II
    tuple_t(double _property_value,
            double _incremental_average,
            size_t _key):
            property_value(_property_value),
            incremental_average(_incremental_average),
            key(_key) {}
};

#endif //SPIKEDETECTION_TUPLE_HPP
