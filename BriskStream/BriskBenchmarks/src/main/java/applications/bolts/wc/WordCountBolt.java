package applications.bolts.wc;

import brisk.components.context.TopologyContext;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;
import util.datatypes.StreamValues;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import constants.WordCountConstants.Field;

public class WordCountBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
    private static final long serialVersionUID = -6454380680803776555L;
    private final Map<String, MutableLong> counts = new HashMap<>();

    public WordCountBolt() {
        super(LOG);
        this.setStateful();
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 80;
        }
        else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        // not in use.
    }

    /**
     * @param input
     * @throws InterruptedException
     */
    @Override
    public void execute(TransferTuple input) throws InterruptedException {
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            char[] word = input.getCharArray(0, i);
            long ts = input.getLong(1, i);
            if (word != null) {
                MutableLong count = counts.computeIfAbsent(new String(word), k -> new MutableLong(0));
                count.increment();
                collector.emit(word, count.longValue(), ts);
            }
        }
    }

    @Override
    public void profile_execute(TransferTuple input) throws InterruptedException {
        int bound = input.length;
        for (int i = 0; i < bound; i++) {
            char[] word = input.getCharArray(0, i);
            long ts = input.getLong(1, i);
            if (word != null) {
                MutableLong count = counts.computeIfAbsent(new String(word), k -> new MutableLong(0));
                count.increment();
                collector.emit(word, count.longValue(), ts);
            }
        }
    }

    public void display() {}

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT, Field.TIMESTAMP);
    }
}
