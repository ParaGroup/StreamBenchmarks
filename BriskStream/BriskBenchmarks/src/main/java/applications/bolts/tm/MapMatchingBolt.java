package applications.bolts.tm;

import brisk.components.operators.base.filterBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.BaseConstants;
import model.gis.GPSRecord;
import model.gis.RoadGridList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.OsUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import static constants.TrafficMonitoringConstants.Conf;
import static constants.TrafficMonitoringConstants.Field;
import constants.TrafficMonitoringConstants.Field;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn
 */
public class MapMatchingBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatchingBolt.class);
    private static final long serialVersionUID = -400404594584158954L;
    int loop;
    private RoadGridList sectors;
    private double cnt1 = 0;

    public MapMatchingBolt() {
        super(LOG, new HashMap<>());//TODO: the output_selectivity here is pre-measured. It shall be profiled.
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 0.3);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        String OS_prefix = null;
        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String shapeFile = config.getString(OS_prefix.concat(Conf.MAP_MATCHER_SHAPEFILE));
        double latMin = config.getDouble(Conf.MAP_MATCHER_LAT_MIN);
        double latMax = config.getDouble(Conf.MAP_MATCHER_LAT_MAX);
        double lonMin = config.getDouble(Conf.MAP_MATCHER_LON_MIN);
        double lonMax = config.getDouble(Conf.MAP_MATCHER_LON_MAX);
        try {
            sectors = new RoadGridList(config, shapeFile);
        }
        catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
        double cnt = 0;
        loops = 5000;//this bolt is too slow, use smaller loops for it.
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//       not in use.
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            int speed = in.getIntegerByField(Field.SPEED, i);
            int bearing = in.getIntegerByField(Field.BEARING, i);
            double latitude = in.getDoubleByField(Field.LATITUDE, i);
            double longitude = in.getDoubleByField(Field.LONGITUDE, i);
            long ts = in.getLong(7, i);
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            int roadID = -1;
            try {
                roadID = sectors.fetchRoadID(record);
            }
            catch (SQLException ex) {
                LOG.error("Unable to fetch road ID", ex);
            }
            if (roadID != -1) {
                Object[] values = new Object[]{in.getMsg(i), roadID};
                collector.emit(0, roadID, speed, true, ts);
            }
            else {
                collector.emit(0, roadID, speed, false, ts);
            }
        }
    }

    public void display() {
//        LOG.info("cnt:" + cnt + "cnt1:" + cnt1 + "Ratio:" + cnt1 / cnt);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ROAD_ID, Field.SPEED, Field.FLAG, Field.TIMESTAMP);
    }
}
