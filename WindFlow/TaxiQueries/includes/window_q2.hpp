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

#ifndef WINDOW_Q2_HPP
#define WINDOW_Q2_HPP

#include<vector>
#include<string>
#include<iterator>
#include<algorithm>
#include<unordered_set>
#include<windflow.hpp>
#include"taxi_ride.hpp"

using namespace wf;

// Window-based functor Q2 (non-incremental version)
class Win_Functor_Q2
{
public:
    // operator() non-incremental version
    void operator()(const Iterable<TaxiRide> &win, TaxiResult &result)
    {
        std::unordered_set<GeoPoint> unique_locations;
        for (const auto &ride: win) {
            unique_locations.insert(ride.location);
        }
        result = TaxiResult::queryQ2(result.area, unique_locations.size(), win.size());
    }
};

#endif
