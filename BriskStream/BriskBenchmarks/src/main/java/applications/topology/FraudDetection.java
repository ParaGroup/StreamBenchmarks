package applications.topology;

import applications.spout.FraudDetectionSpout;
import applications.bolts.comm.FraudDetectionParserBolt;
import applications.bolts.fd.FraudPredictorBolt;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import constants.BaseConstants;
import constants.FraudDetectionConstants;
import constants.FraudDetectionConstants.Component;
import constants.FraudDetectionConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import static constants.FraudDetectionConstants.PREFIX;

public class FraudDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    public FraudDetection(String topologyName, Configuration config) {
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
            int predictor_par_deg = config.getInt(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1);
            int sink_par_deg = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
            // print app info
            LOG.info("Executing FraudDetection with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * predictor: " + predictor_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> predictor -> sink");
            FraudDetectionSpout fdspout = new FraudDetectionSpout(parser, new Fields(Field.ENTITY_ID, Field.RECORD_DATA, Field.TIMESTAMP));
            fdspout.set_GenRate(gen_rate);
            fdspout.setFields(new Fields(Field.ENTITY_ID, Field.RECORD_DATA, Field.TIMESTAMP));
            sink.set_Sampling(sampling);
            builder.setSpout(Component.SPOUT, fdspout, source_par_deg);
            builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt(), predictor_par_deg, new FieldsGrouping(Component.SPOUT, new Fields(Field.ENTITY_ID)));
            builder.setSink(Component.SINK, sink, sink_par_deg, new ShuffleGrouping(Component.PREDICTOR));
/*
            spout.set_GenRate(gen_rate);
            sink.set_Sampling(sampling);
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, source_par_deg);
            builder.setBolt(Component.PARSER, new FraudDetectionParserBolt(parser,
                            new Fields(Field.ENTITY_ID, Field.RECORD_DATA))
                    , source_par_deg
                    , new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt()
                    , predictor_par_deg
                    , new FieldsGrouping(Component.PARSER, new Fields(Field.ENTITY_ID)));

            builder.setSink(Component.SINK, sink, sink_par_deg
                    , new ShuffleGrouping(Component.PREDICTOR)
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
