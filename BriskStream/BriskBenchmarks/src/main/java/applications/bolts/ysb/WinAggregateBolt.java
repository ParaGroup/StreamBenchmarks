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

// class WinAggregateBolt
public class WinAggregateBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WinAggregateBolt.class);
    private static final long serialVersionUID = -5919724558309333175L;
    private HashMap<String, Window> winSet;
    private long discarded;
    private long initialTime;

    public WinAggregateBolt(long _initialTime) {
        super(LOG, new HashMap<>());
        //this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.setStateful();
        this.read_selectivity = 2.0;
        winSet = new HashMap<>();
        discarded = 0;
        initialTime = _initialTime;
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
            char[] cmp_id = in.getCharArray(0, i);
            String cmp_id2 = new String(cmp_id);
            long ts = in.getLong(2, i);
            if (winSet.containsKey(cmp_id2)) {
                // get the current window of key cmp_id
                Window win = winSet.get(cmp_id2);
                if (ts >= win.end_ts) { // window is triggered
                    collector.emit(0, cmp_id2.toCharArray(), win.count, ts);
                    win.count = 1;
                    win.start_ts = (long) ((((ts - initialTime) / 10e09) * 10e09) + initialTime);
                    win.end_ts = (long) (((((ts - initialTime) / 10e09) + 1) * 10e09) + initialTime);
                }
                else if (ts >= win.start_ts) { // window is not triggered
                    win.count++;
                }
                else { // tuple belongs to a previous already triggered window -> it is discarded!
                    discarded++;
                }
            }
            else { // create the first window of the key cmp_id
                winSet.put(cmp_id2, new Window(1, initialTime, (long) (initialTime + 10e09)));
            }
        }
    }

    public void display() {}

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.CMP_ID, Field.COUNT, Field.TIMESTAMP);
    }
}

// window class
class Window {
    public long count;
    public long start_ts;
    public long end_ts;

    public Window(long _count, long _start_ts, long _end_ts) {
        count = _count;
        start_ts = _start_ts;
        end_ts = _end_ts;
    }
}
