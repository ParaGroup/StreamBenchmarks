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
        String BUFFER_SIZE = "tm.buffer_size";
        String POLLING_TIME = "tm.polling_time_ms";
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
