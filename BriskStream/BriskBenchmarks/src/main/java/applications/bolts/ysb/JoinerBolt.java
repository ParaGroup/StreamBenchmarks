package applications.bolts.ysb;

import constants.YSBConstants.Field;
import org.slf4j.Logger;
import java.util.HashMap;
import org.slf4j.LoggerFactory;
import brisk.execution.ExecutionGraph;
import util.Configuration;
import constants.YSBConstants;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.components.operators.base.filterBolt;
import brisk.execution.runtime.tuple.TransferTuple;

// class JoinerBolt
public class JoinerBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(JoinerBolt.class);
    private static final long serialVersionUID = -5919724558309333175L;
    HashMap<String, String> campaignLookup;

    public JoinerBolt(HashMap<String, String> _campaignLookup) {
        super(LOG, new HashMap<>());
        //this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.setStateful();
        this.read_selectivity = 2.0;
        campaignLookup = _campaignLookup;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 27;
        }
        else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {}

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bounds = in.length;
        for (int i = 0; i < bounds; i++) {
            char[] adid = in.getCharArray(2, i);
            String ad_id = new String(adid);
            String campaign_id = campaignLookup.get(ad_id);
            if (campaign_id != null) {
                collector.emit(0, campaign_id.toCharArray(), ad_id.toCharArray(), in.getLong(5, i));
            }
        }
    }

    public void display() {}

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.CMP_ID, Field.AD_ID, Field.TIMESTAMP);
    }
}
