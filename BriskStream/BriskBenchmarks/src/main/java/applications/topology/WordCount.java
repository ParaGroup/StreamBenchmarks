package applications.topology;

import applications.spout.WordCountSpout;
import applications.bolts.wc.SplitSentenceBolt;
import applications.bolts.wc.WordCountBolt;
import applications.sink.MeasureSink;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import constants.BaseConstants;
import constants.WordCountConstants;
import constants.WordCountConstants.Component;
import constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import static constants.WordCountConstants.PREFIX;

public class WordCount extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public WordCount(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {
        try {
            int gen_rate = config.getInt(BaseConstants.BaseConf.GEN_RATE, 1);
            int sampling = config.getInt(BaseConstants.BaseConf.SAMPLING, 1);
            int source_par_deg = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);
            int splitter_par_deg = config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1);
            int counter_par_deg = config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1);
            int sink_par_deg = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
            // print app info
            LOG.info("Executing WordCount with parameters:\n" +
                     "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                     "  * sampling: " + sampling + "\n" +
                     "  * source: " + source_par_deg + "\n" +
                     "  * splitter: " + splitter_par_deg + "\n" +
                     "  * counter: " + counter_par_deg + "\n" +
                     "  * sink: " + sink_par_deg + "\n" +
                     "  * topology: source -> splitter -> counter -> sink");
            WordCountSpout wcspout = new WordCountSpout(parser, new Fields(Field.SENTENCE, Field.TIMESTAMP));
            wcspout.set_GenRate(gen_rate);
            wcspout.setFields(new Fields(Field.SENTENCE, Field.TIMESTAMP));
            MeasureSink wcsink = new MeasureSink("WordCount");
            wcsink.set_Sampling(sampling);
            builder.setSpout(Component.SPOUT, wcspout, source_par_deg);
            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitter_par_deg, new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.COUNTER, new WordCountBolt(), counter_par_deg, new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD)));
            builder.setSink(Component.SINK, wcsink, sink_par_deg, new ShuffleGrouping(Component.COUNTER));
/*
            spout.set_GenRate(gen_rate);
            MeasureSink mysink = new MeasureSink("WordCount");
            mysink.set_Sampling(sampling);

            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, source_par_deg);

            StringParserBolt parserBolt = new StringParserBolt(parser, new Fields(Field.WORD));

            builder.setBolt(Component.PARSER, parserBolt
                    , source_par_deg
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt()
                    , splitter_par_deg
                    , new ShuffleGrouping(Component.PARSER));

            builder.setBolt(Component.COUNTER, new WordCountBolt()
                    , counter_par_deg
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD))
//                  , new ShuffleGrouping(Component.SPOUT)//workaround to ensure balanced scheduling.
            );

            builder.setSink(Component.SINK, mysink, sink_par_deg
                    , new ShuffleGrouping(Component.COUNTER));
//                    , new ShuffleGrouping(Component.SPOUT));
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
