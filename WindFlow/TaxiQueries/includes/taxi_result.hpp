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

#ifndef TAXI_RESULT_HPP
#define TAXI_RESULT_HPP

#include<string>

// class TaxiResult
class TaxiResult
{
public:
    std::string area; // city area
    double medianPassengerCnt; // median number of passengers (Q1)
    int uniqueLocations; // number of unique locations (Q2)
    int count; // number of rides in the window
    double iqr; // inter-quartile range of distances (Q3)
    int wid; // window identifier

    // Constructor I
    TaxiResult():
               area(""),
               medianPassengerCnt(0.0),
               uniqueLocations(0),
               count(0),
               iqr(0.0),
               wid(0) {}

    // Constructor II
    TaxiResult(std::string _area,
               uint64_t _wid):
               area(_area),
               medianPassengerCnt(0.0),
               uniqueLocations(0),
               count(0),
               iqr(0.0), 
               wid(_wid) {}

    // create the TaxiRide event from query Q1
    static TaxiResult queryQ1(const std::string& area,
                              double medianPassengerCnt,
                              int count)
    {
        TaxiResult result;
        result.area = area;
        result.medianPassengerCnt = medianPassengerCnt;
        result.count = count;
        return result;
    }

    // create the TaxiRide event from query Q2
    static TaxiResult queryQ2(const std::string& area,
                              int uniqueCount,
                              int count)
    {
        TaxiResult result;
        result.area = area;
        result.uniqueLocations = uniqueCount;
        result.count = count;
        return result;
    }

    // create the TaxiRide event from query Q3
    static TaxiResult queryQ3(const std::string& area,
                              double iqr,
                              int count)
    {
        TaxiResult result;
        result.area = area;
        result.iqr = iqr;
        result.count = count;
        return result;
    }
};

#endif
