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
import Constants.TrafficMonitoringConstants;
import Constants.TrafficMonitoringConstants.*;
import RoadModel.GPSRecord;
import RoadModel.RoadGridList;
import Util.config.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  This operator receives traces of the vehicles (e.g. through GPS loggers
 *  and GPS phones) including latitude, longitude, speed and direction. These
 *  values are used to determine the location (regarding a road ID) of
 *  the vehicle in real-time.
 */
public class MapMatchingBolt extends BaseRichBolt {
    private static final Logger LOG = Log.get(MapMatchingBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

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

    MapMatchingBolt(String c, int p_deg) {
        city = c;
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples

        roads = new HashMap<>();
        dif_keys = 0;
        all_keys = 0;

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;

        // set city shape file path and city bounding box values
        String city_shapefile;
        city_shapefile = TrafficMonitoringConstants.BEIJING_SHAPEFILE;
        min_lat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LAT, 39.689602);
        max_lat = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LAT, 40.122410);
        min_lon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MIN_LON, 116.105789);
        max_lon = config.getDouble(Conf.MAP_MATCHER_BEIJING_MAX_LON, 116.670021);

        try {
            sectors = new RoadGridList(config, city_shapefile);
        } catch (SQLException | IOException ex) {
            throw new RuntimeException("Error while loading shape file");
        }

        LOG.debug("[MapMatch] Sectors: " + sectors +
                " Bounds (" + city + " case): [" +
                min_lat + ", " + max_lat + "] [" + min_lon + ", " + max_lon + "]");
    }

    @Override
    public void execute(Tuple tuple) {
        String vehicleID = tuple.getStringByField(Field.VEHICLE_ID);
        double latitude = tuple.getDoubleByField(Field.LATITUDE);
        double longitude = tuple.getDoubleByField(Field.LONGITUDE);
        int speed = tuple.getDoubleByField(Field.SPEED).intValue();
        int bearing = tuple.getIntegerByField(Field.BEARING);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);

        LOG.debug("[MapMatch] tuple: vehicleID " + vehicleID +
                 ", lat " + latitude +
                 ", lon " + longitude +
                 ", speed " + speed +
                 ", dir " + bearing +
                 ", ts " + timestamp);

        if (speed < 0) return;
        if (longitude > max_lon || longitude < min_lon || latitude > max_lat || latitude < min_lat) return;

        try {
            // Evaluate roadID given the actual coordinates, speed and direction of the vehicle.
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

            int roadID = sectors.fetchRoadID(record);
            if (roadID != -1) {
                // Road keys statistics
                if (roads.containsKey(roadID)) {
                    int count = roads.get(roadID);
                    roads.put(roadID, count + 1);
                } else {
                    roads.put(roadID, 1);
                    dif_keys++;
                }
                all_keys++;

                collector.emit(tuple, new Values(roadID, speed, timestamp));
            }
        } catch (SQLException e) {
            LOG.error("Unable to fetch road ID", e);
        }
        //collector.ack(tuple);

        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        /*LOG.info("[MapMatch] execution time: " + t_elapsed +
                " ms, processed: " + processed +
                ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                " tuples/s");*/

        /*try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("TMkeys_results_" + city + ".log"));
            bw.write(printKeysStatistics());
            bw.close();
        } catch (IOException e) {
            LOG.error("Error while saving TM keys statistics.");
        }*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.ROAD_ID, Field.SPEED, Field.TIMESTAMP));
    }

    //------------------------------ private methods ---------------------------

    /**
     * Create a string representation of the collected statistics
     * on RoadID keys distribution.
     */
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

    /**
     * Create a string representation of the roads hash map content.
     * @param map roads hash map
     * @param size number of all keys
     * @return representation of the hash map content
     */
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
