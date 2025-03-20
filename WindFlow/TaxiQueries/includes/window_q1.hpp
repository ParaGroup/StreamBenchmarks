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

#ifndef WINDOW_Q1_HPP
#define WINDOW_Q1_HPP

#include<vector>
#include<string>
#include<iterator>
#include<algorithm>
#include<windflow.hpp>
#include"taxi_ride.hpp"

using namespace wf;

// Window-based functor Q1 (non-incremental version)
class Win_Functor_Q1
{
public:
    // operator() non-incremental version
    void operator()(const Iterable<TaxiRide> &win, TaxiResult &result)
    {
        std::vector<long> passengerCounts;
        for (const auto &ride: win) {
            if (!ride.isStart) {
                passengerCounts.push_back(ride.passengerCnt);
            }
        }
        if (passengerCounts.size() == 0) {
            result = TaxiResult::queryQ1(result.area, 0.0, 0);
        }
        else {
            std::sort(passengerCounts.begin(), passengerCounts.end());
            double median;
            if (passengerCounts.size() % 2 == 0) {
                median = (passengerCounts[passengerCounts.size() / 2 - 1] + passengerCounts[passengerCounts.size() / 2]) / 2.0;
            }
            else {
                median = passengerCounts[passengerCounts.size() / 2];
            }
            result = TaxiResult::queryQ1(result.area, median, passengerCounts.size());
        }        
    }
};

#endif
