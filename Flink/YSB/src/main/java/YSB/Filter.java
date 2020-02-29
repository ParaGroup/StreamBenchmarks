package YSB;

import Util.Log;
import java.util.*;
import org.slf4j.Logger;
import Constants.YSBConstants;
import org.slf4j.LoggerFactory;
import Constants.YSBConstants.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFilterFunction;

// class Filter
public class Filter extends RichFilterFunction<YSB_Event> {
    private static final Logger LOG = Log.get(Filter.class);
    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;

    // constructor
    Filter(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;
    }

    // flatmap method
    @Override
    public boolean filter(YSB_Event input) {
        String uuid = input.uuid1;
        String uuid2 = input.uuid2;
        String ad_id = input.ad_id;
        String ad_type = input.ad_type;
        String event_type = input.event_type;
        long ts = input.timestamp;
        String ip = input.ip;
        t_end = System.nanoTime();
        processed++;
        if (event_type.equals("view")) {
            return true;
        }
        else {
            return false;        
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[Filter] execution time: " + t_elapsed + " ms, " +
                            "processed: " + processed + ", " +
                            "bandwidth: " + processed / (t_elapsed / 1000) + " (tuples/s)");*/
    }
}
