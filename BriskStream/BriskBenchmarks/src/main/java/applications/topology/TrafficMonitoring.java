package applications.topology;

import applications.spout.TrafficMonitoringSpout;
import applications.bolts.comm.TrafficMonitoringParser;
import applications.bolts.tm.MapMatchingBolt;
import applications.bolts.tm.SpeedCalculatorBolt;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import constants.BaseConstants;
import constants.TrafficMonitoringConstants;
import brisk.controller.input.scheduler.SequentialScheduler;
import static constants.TrafficMonitoringConstants.*;

/**
 * https://github.com/whughchen/RealTimeTraffic
 *
 * @author Chen Guanghua <whughchen@gmail.com>
 */
public class TrafficMonitoring extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private final int mapMatcherThreads;
    private final int speedCalcThreads;

    public TrafficMonitoring(String topologyName, Configuration config) {
        super(topologyName, config);
        initilize_parser();
        mapMatcherThreads = config.getInt(Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads = config.getInt(Conf.SPEED_CALCULATOR_THREADS, 1);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
        // sink.loops = 20;//TM is too slow. TODO: disabled for upper bound test purpose
    }

    @Override
    public Topology buildTopology() {
        try {
            int gen_rate = config.getInt(BaseConstants.BaseConf.GEN_RATE, 1);
            int sampling = config.getInt(BaseConstants.BaseConf.SAMPLING, 1);
            int source_par_deg = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);
            int mm_par_deg = config.getInt(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, 1);
            int sc_par_deg = config.getInt(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, 1);
            int sink_par_deg = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
            // print app info
            LOG.info("Executing TrafficMonitoring with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * map-matcher: " + mm_par_deg + "\n" +
                     "  * speed-calculator: " + sc_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> map-matcher -> speed-calculator -> sink");
            TrafficMonitoringSpout tmspout = new TrafficMonitoringSpout(parser, new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED, Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.TIMESTAMP));
            tmspout.set_GenRate(gen_rate);
            tmspout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED, Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.TIMESTAMP));
            sink.set_Sampling(sampling);
            builder.setSpout(Component.SPOUT, tmspout, source_par_deg);
            builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mm_par_deg, new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), sc_par_deg, new FieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID)));
            builder.setSink(Component.SINK, sink, sink_par_deg, new ShuffleGrouping(Component.SPEED_CALCULATOR));
/*
            spout.set_GenRate(gen_rate);
            sink.set_Sampling(sampling);

            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, source_par_deg);

            builder.setBolt(Component.PARSER, new TrafficMonitoringParser(parser,
                            new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                                    Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.ROAD_ID))
                    , source_par_deg
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mm_par_deg,
                    new ShuffleGrouping(Component.PARSER));

            builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), sc_par_deg,
                    new FieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID)));

            builder.setSink(Component.SINK, sink, sink_par_deg,
                    new ShuffleGrouping(Component.SPEED_CALCULATOR));
*/
        }
        catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

}
