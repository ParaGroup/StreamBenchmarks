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

#ifndef TRAFFICMONITORING_MAP_MATCHER_HPP
#define TRAFFICMONITORING_MAP_MATCHER_HPP

#include <ff/ff.hpp>

#include "ogr_geometry.h"
#include "../util/tuple.hpp"
#include "../util/result.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"
#include "../geographic_model/road_grid_list.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/**
 *  @class Map_Matcher_Functor
 *
 *  @brief Define the logic of the Map Matcher
 */
class Map_Matcher_Functor {
private:
    size_t processed;                            // counter of processed tuples
    size_t valid_points;                         // counter of tuples containing GPS coordinates (points) laying inside the city bounding box
    size_t emitted;                              // counter of tuples containing points that correspond to a valid road_id
    Road_Grid_List road_grid_list;               // object containing all the geometric features of the shapefile and used to do map matching
    unordered_map<size_t, uint64_t> key_occ;     // contains the number of occurrences of each key road_id

    // city bounding box
    double max_lon;
    double min_lon;
    double max_lat;
    double min_lat;

    // time variables
    unsigned long app_start_time;
    unsigned long current_time;

    // runtime information
    size_t parallelism;
    size_t replica_id;

public:

    /**
     *  @brief Constructor
     */
    Map_Matcher_Functor(Road_Grid_List& _road_grid_list, const unsigned long _app_start_time):
                road_grid_list(_road_grid_list),
                processed(0),
                valid_points(0),
                emitted(0),
                app_start_time(_app_start_time),
                current_time(_app_start_time)
    {
        // set city bounding box
        max_lon = (_monitored_city == DUBLIN) ? dublin_lon_max : beijing_lon_max;
        min_lon = (_monitored_city == DUBLIN) ? dublin_lon_min : beijing_lon_min;
        max_lat = (_monitored_city == DUBLIN) ? dublin_lat_max : beijing_lat_max;
        min_lat = (_monitored_city == DUBLIN) ? dublin_lat_min : beijing_lat_min;
    }

    /**
     *  @brief Compute a road ID matching the GPS coordinates of a vehicle
     *
     *  Evaluate for each input tuple containing the GPS coordinates received from a vehicle the
     *  road ID corresponding to the position of the vehicle in the map of the city.
     *  @param t input tuple
     *  @param rc runtime context used to access to the parallelism degree and replica index
     */
    void operator()(const tuple_t& t, Shipper<result_t>& shipper, RuntimeContext& rc) {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        //print_tuple("[MapMatcher] Received tuple: ", t);

        if (t.speed >= 0 &&
            t.longitude <= max_lon && t.longitude >= min_lon &&
            t.latitude <= max_lat && t.latitude >= min_lat)
        {
            OGRPoint p(t.longitude, t.latitude);
            int road_id = road_grid_list.fetch_road_ID(p);
            if (road_id != -1) {
                // road_id keys
                if (key_occ.find(road_id) == key_occ.end())
                    key_occ.insert(make_pair(road_id, 0));

                result_t r;
                r.speed = t.speed;
                r.key = road_id;
                r.ts = t.ts;
                shipper.push(std::move(r));
                emitted++;

                //print_result("[MapMatcher] Result: ", r);
            }
            valid_points++;
        }
        processed++;
        current_time = current_time_nsecs();
    }

    /**
     *  @brief Destructor.
     */
    ~Map_Matcher_Functor() {
        //if (processed != 0) {
            /*cout << "[MapMatcher] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1e09
                 << " s, processed: " << processed
                 << ", valid (inside city box): " << valid_points
                 << ", emitted (#all_keys): " << emitted
                 << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                 << ", #dif_keys: " << key_occ.size()
                 << endl;*/

            // print road_id keys and number of occurrences
            /*     << ", keys: "
                 << endl;
            for (auto k : key_occ) {
                cout << "key: " << k.first << " id: " << k.second << endl;
            }*/
        //}
    }
};

#endif //TRAFFICMONITORING_MAP_MATCHER_HPP
