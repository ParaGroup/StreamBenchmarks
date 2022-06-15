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

package TrafficMonitoring;

import Util.Log;
import java.util.Map;
import org.slf4j.Logger;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.IOException;
import RoadModel.GPSRecord;
import java.sql.SQLException;
import RoadModel.RoadGridList;
import java.io.BufferedWriter;
import org.slf4j.LoggerFactory;
import java.text.DecimalFormat;
import org.apache.flink.util.Collector;
import Constants.TrafficMonitoringConstants;
import Constants.TrafficMonitoringConstants.City;
import Constants.TrafficMonitoringConstants.Conf;
import Constants.TrafficMonitoringConstants.Field;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  This operator receives traces of the vehicles (e.g. through GPS loggers
 *  and GPS phones) including latitude, longitude, speed and direction. These
 *  values are used to determine the location (regarding a road ID) of
 *  the vehicle in real-time.
 */ 
public class MapMatchingCalculator extends RichFlatMapFunction<Source_Event, Output_Event> {
    private static final Logger LOG = Log.get(MapMatchingCalculator.class);
    private String city;
    private RoadGridList sectors;
    private double min_lat;
    private double max_lat;
    private double min_lon;
    private double max_lon;
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;
    // state of the bolt (contains statistics about the distribution of roadID keys)
    private HashMap<Integer, Integer> roads;
    private int dif_keys;
    private int all_keys;
    private Configuration config;

    // Constructor
    public MapMatchingCalculator(String c, int p_deg, Configuration _config) {
        city = c;
        par_deg = p_deg;     // bolt parallelism degree
        config = _config;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        roads = new HashMap<>();
        dif_keys = 0;
        all_keys = 0;
        // set city shape file path and city bounding box values
        String city_shapefile;
        city_shapefile = TrafficMonitoringConstants.BEIJING_SHAPEFILE;
        min_lat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LAT, 39.689602);
        max_lat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LAT, 40.122410);
        min_lon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LON, 116.105789);
        max_lon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LON, 116.670021);
        try {
            sectors = new RoadGridList(config, city_shapefile);
        }
        catch (SQLException | IOException ex) {
            throw new RuntimeException("Error while loading shape file");
        }
        LOG.debug("[MapMatchingBolt] Sectors: " + sectors +
                " Bounds (" + city + " case): [" +
                min_lat + ", " + max_lat + "] [" + min_lon + ", " + max_lon + "]");
    }

    // flatmap method
    @Override
    public void flatMap(Source_Event input, Collector<Output_Event> output) {
        String vehicleID = input.vehicleID;
        double latitude = input.latitude;
        double longitude = input.longitude;
        int speed = (int) input.speed;
        int bearing = input.bearing;
        long timestamp = input.ts;
        LOG.debug("[MapMatch] tuple: vehicleID " + vehicleID +
                ", lat " + latitude +
                ", lon " + longitude +
                ", speed " + speed +
                ", dir " + bearing +
                ", ts " + timestamp);
        if (speed < 0)
            return;
        if (longitude > max_lon || longitude < min_lon || latitude > max_lat || latitude < min_lat)
            return;
        try {
            // Evaluate roadID given the actual coordinates, speed and direction of the vehicle.
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            int roadID = sectors.fetchRoadID(record);
            if (roadID != -1) {
                // Road keys statistics
                if (roads.containsKey(roadID)) {
                    int count = roads.get(roadID);
                    roads.put(roadID, count + 1);
                }
                else {
                    roads.put(roadID, 1);
                    dif_keys++;
                }
                all_keys++;
                output.collect(new Output_Event(roadID, speed, timestamp));
            }
        }
        catch (SQLException e) {
            LOG.error("Unable to fetch road ID", e);
        }
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[MapMatch] execution time: " + t_elapsed +
                            " ms, processed: " + processed +
                            ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");*/
        /*try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("TMkeys_results_" + city + ".log"));
            bw.write(printKeysStatistics());
            bw.close();
        }
        catch (IOException e) {
            LOG.error("Error while saving TM keys statistics.");
        }*/
    }

    // printKeysStatistics method
    private String printKeysStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append("RoadID keys statistics:")
                .append("\n* total number of keys: ")
                .append(all_keys)
                .append("\n* number of different keys: ")
                .append(dif_keys)
                .append("\n* distribution: \n")
                .append(printMap(roads, all_keys));
        return sb.toString();
    }

    // printMap method
    private static String printMap(Map<Integer, Integer> map, int size) {
        StringBuilder sb = new StringBuilder();
        DecimalFormat df = new DecimalFormat("#.#####");
        for (Integer k : map.keySet()) {
            sb.append("key ")
                    .append(k)
                    .append(" appeared ")
                    .append(df.format(((double)map.get(k) * 100) / size))
                    .append("% of times.\n");
        }
        return sb.toString();
    }
}
