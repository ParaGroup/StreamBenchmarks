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

package Constants;

/** 
 *  @author Alessandra Fais
 *  @version June 2019
 *  
 *  Constants peculiar of the TrafficMonitoring application.
 */ 
public interface TrafficMonitoringConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/trafficmonitoring/tm.properties";
    String DEFAULT_TOPO_NAME = "TrafficMonitoring";
    String BEIJING_SHAPEFILE = "../../Datasets/TM/beijing/roads.shp";

    interface Conf {
        String RUNTIME = "tm.runtime_sec";
        String SPOUT_BEIJING = "tm.spout.beijing";
        String MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile";
        String MAP_MATCHER_BEIJING_MIN_LAT = "tm.map_matcher.beijing.lat.min";
        String MAP_MATCHER_BEIJING_MAX_LAT = "tm.map_matcher.beijing.lat.max";
        String MAP_MATCHER_BEIJING_MIN_LON = "tm.map_matcher.beijing.lon.min";
        String MAP_MATCHER_BEIJING_MAX_LON = "tm.map_matcher.beijing.lon.max";
        String ROAD_FEATURE_ID_KEY    = "tm.road.feature.id_key";
        String ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key";
    }

    interface Component extends BaseComponent {
        String MAP_MATCHER = "map_matcher_bolt";
        String SPEED_CALCULATOR = "speed_calculator_bolt";
    }

    interface Field extends BaseField {
        String VEHICLE_ID = "vehicleID";
        String SPEED = "speed";
        String BEARING = "bearing";
        String LATITUDE = "latitude";
        String LONGITUDE = "longitude";
        String ROAD_ID = "roadID";
        String AVG_SPEED = "averageSpeed";
        String COUNT = "count";
    }

    // cities supported by the application
    interface City {
        String BEIJING = "beijing";
    }

    // constants used to parse Beijing taxi traces
    interface BeijingParsing {
        int B_VEHICLE_ID_FIELD = 0; // carID
        int B_NID_FIELD = 1;
        int B_DATE_FIELD = 2;
        int B_LATITUDE_FIELD = 3;
        int B_LONGITUDE_FIELD = 4;
        int B_SPEED_FIELD = 5;
        int B_DIRECTION_FIELD = 6;
    }
}
