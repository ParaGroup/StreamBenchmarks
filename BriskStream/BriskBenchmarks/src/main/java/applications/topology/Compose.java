package applications.topology;

import applications.bolts.comm.GeneralParserBolt;
import applications.bolts.wc.SplitSentenceBolt;
import applications.bolts.wc.WordCountBolt;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import constants.WordCountConstants;
import constants.WordCountConstants.Component;
import constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import static constants.WordCountConstants.PREFIX;

public class Compose extends CompositeTopology {
    private static final Logger LOG = LoggerFactory.getLogger(Compose.class);

    private Compose(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
    }

    public void initialize() {
        super.initialize();//here, we initialize multiple spouts.
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {
        try {
//            spout.setFields(new Fields(Field.TEXT));
//            builder.setSpout(Component.SPOUT, spout, spoutThreads);

            builder.setBolt(Component.PARSER, new GeneralParserBolt(parser, new Fields(Field.WORD))
                    , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt()
                    , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1)
                    , new ShuffleGrouping(Component.PARSER));
//
            builder.setBolt(Component.COUNTER, new WordCountBolt()
                    , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1)
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD)));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.COUNTER));
//                    , new ShuffleGrouping(Component.SPOUT));

        } catch (InvalidIDException e) {
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
