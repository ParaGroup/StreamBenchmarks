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

#ifndef WINDOW_Q3_HPP
#define WINDOW_Q3_HPP

#include<vector>
#include<string>
#include<iterator>
#include<algorithm>
#include<windflow.hpp>
#include"taxi_ride.hpp"

using namespace wf;

// Window-based functor Q3 (non-incremental version)
class Win_Functor_Q3
{
private:
    // method to compute a given percentile
    double calculatePercentile(const std::vector<double> &sortedData,
                               double percentile)
    {
        size_t n = sortedData.size();
        double index = (percentile / 100.0) * (n - 1);
        size_t lowerIndex = static_cast<size_t>(index);
        size_t upperIndex = lowerIndex + 1;
        double fraction = index - lowerIndex;
        if (upperIndex >= n) {
            return sortedData[lowerIndex];
        }
        else {
            return sortedData[lowerIndex] + fraction * (sortedData[upperIndex] - sortedData[lowerIndex]);
        }
    }

public:
    // operator() non-incremental version
    void operator()(const Iterable<TaxiRide> &win, TaxiResult &result)
    {
        std::vector<double> distances;
        for (const auto &ride: win) {
            if (!ride.isStart) {
                distances.push_back(ride.travelDist);
            }
        }
        if (distances.size() == 0) {
            result = TaxiResult::queryQ3(result.area, 0.0, 0);
        }
        else {
            std::sort(distances.begin(), distances.end());
            double Q1 = calculatePercentile(distances, 25);
            double Q3 = calculatePercentile(distances, 75);
            double IQR = Q3 - Q1;
            result = TaxiResult::queryQ3(result.area, IQR, distances.size());
        }
    }
};

#endif
