package applications.bolts.wc;

import brisk.components.context.TopologyContext;
import brisk.components.operators.base.splitBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.BaseConstants;
import constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;
import org.apache.commons.lang3.StringUtils;
import java.util.HashMap;
import constants.WordCountConstants.Field;

public class SplitSentenceBolt extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private static final long serialVersionUID = 8089145995668583749L;

    public SplitSentenceBolt() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
        OsUtils.configLOG(LOG);
    }

    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 10;
        }
        else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // not in use.
    }

    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] sentence = in.getCharArray(0, i);
            long ts = in.getLong(1, i);
            if (sentence != null) {
                String[] split = new String(sentence).split(" ");
                for (String word : split) {
                    //if (!StringUtils.isBlank(word)) {
                        //String word2 = "elia";
                        collector.emit(0, word.toCharArray(), ts);
                    //}
                }
            }
        }
    }

    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] sentence = in.getCharArray(0, i);
            long ts = in.getLong(1, i);
            if (sentence != null) {
                String[] split = new String(sentence).split(" ");
                for (String word : split) {
                    //if (!StringUtils.isBlank(word)) {
                        //String word2 = "elia";
                        collector.emit(0, word.toCharArray(), ts);
                    //}
                }
            }
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.TIMESTAMP);
    }
}
