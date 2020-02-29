package applications.topology;

import applications.spout.SpikeDetectionSpout;
import applications.bolts.comm.SensorParserBolt;
import applications.bolts.sd.MovingAverageBolt;
import applications.bolts.sd.SpikeDetectionBolt;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import constants.BaseConstants;
import constants.SpikeDetectionConstants;
import constants.SpikeDetectionConstants.Component;
import constants.SpikeDetectionConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import static constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import static constants.SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS;
import static constants.SpikeDetectionConstants.PREFIX;

public class SpikeDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);

    public SpikeDetection(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public Topology buildTopology() {
        try {
            int gen_rate = config.getInt(BaseConstants.BaseConf.GEN_RATE, 1);
            int sampling = config.getInt(BaseConstants.BaseConf.SAMPLING, 1);
            int source_par_deg = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);
            int ma_par_deg = config.getInt(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, 1);
            int sd_par_deg = config.getInt(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, 1);
            int sink_par_deg = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
            // print app info
            LOG.info("Executing SpikeDetection with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * moving-average: " + ma_par_deg + "\n" +
                     "  * spike-detector: " + sd_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> moving-average -> spike-detector -> sink");
            SpikeDetectionSpout sdspout = new SpikeDetectionSpout(parser, new Fields(Field.DEVICE_ID, Field.VALUE, Field.TIMESTAMP));
            sdspout.set_GenRate(gen_rate);
            sdspout.setFields(new Fields(Field.DEVICE_ID, Field.VALUE, Field.TIMESTAMP));
            sink.set_Sampling(sampling);
            builder.setSpout(Component.SPOUT, sdspout, source_par_deg);
            builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(), ma_par_deg, new FieldsGrouping(Component.SPOUT, new Fields(Field.DEVICE_ID)));
            builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(), sd_par_deg, new ShuffleGrouping(Component.MOVING_AVERAGE));
            builder.setSink(Component.SINK, sink, sink_par_deg, new ShuffleGrouping(Component.SPIKE_DETECTOR));
/*
            spout.set_GenRate(gen_rate);
            sink.set_Sampling(sampling);

            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, source_par_deg);

            builder.setBolt(Component.PARSER, new SensorParserBolt(parser, new Fields(Field.DEVICE_ID, Field.VALUE))
                    , source_par_deg
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(),
                    ma_par_deg
                    , new FieldsGrouping(Component.PARSER, new Fields(Field.DEVICE_ID)
                    ));

            builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(),
                    sd_par_deg
                    , new ShuffleGrouping(Component.MOVING_AVERAGE));

            builder.setSink(Component.SINK, sink, sink_par_deg
                    , new ShuffleGrouping(Component.SPIKE_DETECTOR)
//                    , new MarkerShuffleGrouping(Component.SPIKE_DETECTOR)
            );
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
