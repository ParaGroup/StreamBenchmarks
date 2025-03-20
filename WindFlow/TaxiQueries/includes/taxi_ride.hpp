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

#ifndef TAXI_RIDE_HPP
#define TAXI_RIDE_HPP

#include<ctime>
#include<locale>
#include<string>
#include<vector>
#include<iomanip>
#include<sstream>
#include<iostream>

// class GeoPoint
class GeoPoint
{
public:
    double lon;
    double lat;

    // Constructor I
    GeoPoint(): lon(0.0), lat(0.0) {}

    // Constructor II
    GeoPoint(double _lon, double _lat): lon(_lon), lat(_lat) {}

    // Override equals method
    bool operator==(const GeoPoint &other) const
    {
        return lon == other.lon && lat == other.lat;
    }

    // Override toString method
    std::string toString() const
    {
        std::ostringstream oss;
        oss << "{" << lon << ", " << lat << "}";
        return oss.str();
    }
};

// class TaxiRide
class TaxiRide
{
public:
    long rideId; // unique identifier of each ride
    std::tm time; // timestamp of the start/end event
    bool isStart; // true if the ride starts from the location, otherwise it is false
    GeoPoint location; // location of pick-up/drop-off of the ride
    long passengerCnt; // number of passengers
    double travelDist; // travel distance (0 if isStart is true)
    std::string area; // label corresponding to the city area

    // Constructor I
    TaxiRide():
             rideId(0),
             isStart(false),
             passengerCnt(0),
             travelDist(0.0),
             area("") {}

    // Constructor II
    TaxiRide(long _rideId,
             std::tm _time,
             bool _isStart,
             GeoPoint _location,
             long _passengerCnt,
             double _travelDist,
             std::string _area):
             rideId(_rideId),
             time(_time),
             isStart(_isStart),
             location(_location),
             passengerCnt(_passengerCnt),
             travelDist(_travelDist),
             area(_area) {}

    // create the TaxiRide event from a string
    static TaxiRide fromString(const std::string &line)
    {
        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> tokens;
        while (std::getline(iss, token, ',')) {
            tokens.push_back(token);
        }
        if (tokens.size() != 8) {
            throw std::runtime_error("Invalid record: " + line);
        }
        long rideId = std::stol(tokens[0]);
        std::tm time = {};
        std::istringstream ss(tokens[1]);
        ss >> std::get_time(&time, "%Y-%m-%d %H:%M:%S");
        bool isStart = tokens[2] == "START";
        double lon = tokens[3].empty() ? 0.0 : std::stod(tokens[3]);
        double lat = tokens[4].empty() ? 0.0 : std::stod(tokens[4]);
        long passengerCnt = std::stol(tokens[5]);
        double travelDistance = tokens[6].empty() ? 0.0 : std::stod(tokens[6]);
        std::string area = tokens[7];
        area.pop_back(); // the last two char are garbage (to be investigated why...)
        area.pop_back(); // the last two char are garbage (to be investigated why...)
        return TaxiRide(rideId, time, isStart, GeoPoint(lon, lat), passengerCnt, travelDistance, area);
    }

	// create a string representing the ride
	std::string toString() const
	{
	    std::ostringstream oss;
	    oss << "TaxiRide {"
	        << "rideId: " << rideId
	        << ", time: " << std::put_time(&time, "%Y-%m-%d %H:%M:%S")
	        << ", isStart: " << (isStart ? "START" : "END")
	        << ", location: " << location.toString()
	        << ", passengerCnt: " << passengerCnt
	        << ", travelDist: " << travelDist
	        << ", area: " << area
	        << "}";
	    return oss.str();
	}
};

// Hash function per GeoPoint
namespace std {
    template<>
    struct hash<GeoPoint>
    {
        std::size_t operator()(const GeoPoint &gp) const
        {
            return std::hash<double>()(gp.lat) ^ std::hash<double>()(gp.lon);
        }
    };
}

#endif
