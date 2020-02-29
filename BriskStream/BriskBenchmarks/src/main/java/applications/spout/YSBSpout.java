package applications.spout;

import java.io.*;
import java.util.*;
import org.slf4j.Logger;
import java.util.Scanner;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import util.OsUtils;
import util.CampaignAd;
import brisk.execution.ExecutionGraph;
import util.Configuration;
import java.lang.management.RuntimeMXBean;
import constants.BaseConstants;
import java.lang.management.ManagementFactory;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.AbstractSpout;

// class YSBSpout
public class YSBSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(YSBSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    private List<CampaignAd> campaigns;
    private final static String CAMPAIGNS_TOPIC = "campaigns";
    private final static String EVENTS_TOPIC = "events";
    private final static String OUTPUT_TOPIC = "output";
    private final static List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    private final static List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");
    private String uuid;
    private int adTypeLength;
    private int eventTypeLength;
    private int campaignLength;
    private int i;
    private int j;
    private int k;
    private long ts;
    private long initialTime;
    private long sent = 0;

    public YSBSpout() {
        super(LOG);
        this.scalable = false;
    }

    public void setCampaigns(List<CampaignAd> _campaigns) {
        campaigns = _campaigns;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        }
        else {
            return 1;
        }
    }

    public void setInitialTime(long _initialTime) {
        this.initialTime = _initialTime;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        uuid = UUID.randomUUID().toString(); // used as a dummy value for all events, based on ref code
        adTypeLength = AD_TYPES.size();
        eventTypeLength = EVENT_TYPES.size();
        campaignLength = campaigns.size();
        i = 0;
        j = 0;
        k = 0;
    }

    @Override
    public void cleanup() {}

    @Override
    public void nextTuple() throws InterruptedException {
        i += 1;
        j += 1;
        k += 1;
        if (i >= campaignLength) {
            i = 0;
        }
        if (j >= adTypeLength) {
            j = 0;
        }
        if (k >= eventTypeLength) {
            k = 0;
        }
        sent++;
        ts = initialTime + (System.nanoTime() - initialTime);
        String ad_id = (campaigns.get(i)).ad_id; // ad id for the current event index
        String ad_type = AD_TYPES.get(j); // current adtype for event index
        String event_type = EVENT_TYPES.get(k); // current event type for event index
        String ip = "255.255.255.255";
        collector.emit(0, uuid.toCharArray(), uuid.toCharArray(), ad_id.toCharArray(), ad_type.toCharArray(), event_type.toCharArray(), ts, ip.toCharArray());
        if (gen_rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
            active_delay(delay_nsec);
        }
    }

    @Override
    public void nextTuple_nonblocking() throws InterruptedException {
        nextTuple();
    }

    @Override
    public long getNumTuples() {
        return sent;
    }

    public void display() {}

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }
}
