package applications.bolts.ysb;

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

// class FilterBolt
public class FilterBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);
    private static final long serialVersionUID = -5919724558309333175L;

    public FilterBolt() {
        super(LOG, new HashMap<>());
        //this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.setStateful();
        this.read_selectivity = 2.0;
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
            char[] ev = in.getCharArray(4, i);
            String event_id = new String(ev);
            if (event_id.equals("view")) {
                collector.emit(0, in.getCharArray(0, i), in.getCharArray(1, i), in.getCharArray(2, i), in.getCharArray(3, i), in.getCharArray(4, i), in.getLong(5, i), in.getCharArray(6, i));
            }
        }
    }

    public void display() {}
}
