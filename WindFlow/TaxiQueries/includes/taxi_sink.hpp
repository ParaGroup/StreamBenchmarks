/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
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

#ifndef TAXI_SINK_HPP
#define TAXI_SINK_HPP

#include<string>
#include<memory>
#include<optional>
#include<iostream>
#include<windflow.hpp>
#include"taxi_ride.hpp"
#include"taxi_result.hpp"

using namespace wf;

#if 1
// Sink_Functor class
class TaxiSink_Functor
{
private:
    int received_tuples; // number of received results
    int query_id; // query identifier

public:
    // Constructor
    TaxiSink_Functor(int _query_id):
                     received_tuples(0),
                     query_id(_query_id) {}

    // operator() method
    void operator()(std::optional<TaxiResult> &input, RuntimeContext &rc)
    {
        if (input) {
            received_tuples++;
            if (query_id == 1) {
                // if (received_tuples < 100)
                	// std::cout << "[SINK] Received window result: [area: " << input->area << ", medianPassengerCnt: " << input->medianPassengerCnt << ", count: " << input->count << "]" << std::endl;
            }
            else if (query_id == 2) {
                // std::cout << "[SINK] Received window result: [area: " << input->area << ", uniqueLocations: " << input->uniqueLocations << ", count: " << input->count << "]" << std::endl;
            }
            else if (query_id == 3) {
                // std::cout << "[SINK] Received window result: [area: " << input->area << ", iqr: " << input->iqr << ", count: " << input->count << "]" << std::endl;
            }
        }
        else { // EOS
            // ...
        }
    }
};
#else
// Sink_Functor class
class TaxiSink_Functor
{
private:
    int received_tuples; // number of received results
    int query_id; // query identifier

public:
    // Constructor
    TaxiSink_Functor(int _query_id):
                     received_tuples(0),
                     query_id(query_id) {}

    // operator() method
    void operator()(std::optional<TaxiRide> &input, RuntimeContext &rc)
    {
        if (input) {
            received_tuples++;
            // std::cout << "SINK: received ride -> [id: " << input->rideId << ", isStart: " << input->isStart << ", location: " << (input->location).toString() << ", passengerCnt: " << input-> passengerCnt << ", travelDist: " << input-> travelDist << ", area: " << input-> area << "]" << std::endl;
        }
        else { // EOS
            // ...
        }
    }
};
#endif

#endif
