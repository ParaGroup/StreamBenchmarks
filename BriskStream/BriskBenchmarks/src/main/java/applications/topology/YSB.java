package applications.topology;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import brisk.components.Topology;
import applications.spout.YSBSpout;
import brisk.topology.BasicTopology;
import util.CampaignAd;
import java.util.concurrent.TimeUnit;
import util.Configuration;
import brisk.components.operators.api.*;
import constants.YSBConstants;
import applications.bolts.ysb.FilterBolt;
import applications.bolts.ysb.JoinerBolt;
import applications.bolts.ysb.WinAggregateBolt;
import brisk.components.grouping.FieldsGrouping;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.components.grouping.ShuffleGrouping;
import constants.YSBConstants.Field;
import constants.BaseConstants;
import applications.sink.MeasureSink;
import constants.YSBConstants.Component;
import brisk.components.exception.InvalidIDException;
import brisk.components.operators.api.BaseWindowedBolt;
import brisk.controller.input.scheduler.SequentialScheduler;
import applications.spout.YSBSpout;
import static constants.YSBConstants.PREFIX;

// class YSB
public class YSB extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(YSB.class);

    public YSB(String topologyName, Configuration config) {
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
        int gen_rate = config.getInt(BaseConstants.BaseConf.GEN_RATE, 1);
        int sampling = config.getInt(BaseConstants.BaseConf.SAMPLING, 1);
        int source_par_deg = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);
        int filter_par_deg = config.getInt(YSBConstants.Conf.FILTER_THREADS, 1);
        int joiner_par_deg = config.getInt(YSBConstants.Conf.JOINER_THREADS, 1);
        int agg_par_deg = config.getInt(YSBConstants.Conf.AGGREGATE_THREADS, 1);
        int sink_par_deg = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
        // print app info
        LOG.info("Executing YSB with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + sampling + "\n" +
                 "  * source: " + source_par_deg + "\n" +
                 "  * filter: " + filter_par_deg + "\n" +
                 "  * joiner: " + joiner_par_deg + "\n" +
                 "  * win-aggregate: " + agg_par_deg + "\n" +
                 "  * sink: " + sink_par_deg + "\n" +
                 "  * topology: source -> filter -> joiner -> win-aggregate -> sink");
        final int numCampaigns = config.getInt(YSBConstants.Conf.NUM_KEYS, 1);
        List<CampaignAd> campaignAdSeq = generateCampaignMapping(numCampaigns);
        HashMap<String, String> campaignLookup = new HashMap<String, String>();
        for (int i=0; i<campaignAdSeq.size(); i++) {
            campaignLookup.put((campaignAdSeq.get(i)).ad_id, (campaignAdSeq.get(i)).campaign_id);
        }
        YSBSpout ysbspout = new YSBSpout();
        ysbspout.set_GenRate(gen_rate);
        ysbspout.setCampaigns(campaignAdSeq);
        long initialTime = System.nanoTime();
        ysbspout.setInitialTime(initialTime);
        MeasureSink ysbsink = new MeasureSink("YSB");
        ysbsink.set_Sampling(sampling);
        try {
            builder.setSpout(Component.SPOUT, ysbspout, source_par_deg);
            builder.setBolt(Component.FILTER, new FilterBolt(), filter_par_deg, new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.JOINER, new JoinerBolt(campaignLookup), joiner_par_deg, new ShuffleGrouping(Component.FILTER));
            builder.setBolt(Component.AGGREGATE, new WinAggregateBolt(initialTime), agg_par_deg, new FieldsGrouping(Component.JOINER, new Fields(Field.CMP_ID)));
            builder.setSink(Component.SINK, ysbsink, sink_par_deg, new ShuffleGrouping(Component.AGGREGATE));
        }
        catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    // generate in-memory ad_id to campaign_id map
    private static List<CampaignAd> generateCampaignMapping(int numCampaigns) {
        CampaignAd[] campaignArray = new CampaignAd[numCampaigns*10];
        for (int i=0; i<numCampaigns; i++) {
            String campaign = UUID.randomUUID().toString();
            for (int j=0; j<10; j++) {
                campaignArray[(10*i)+j] = new CampaignAd(UUID.randomUUID().toString(), campaign);
            }
        }
        return Arrays.asList(campaignArray);
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
