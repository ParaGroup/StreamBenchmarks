package applications.bolts.vs;


import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import model.cdr.CallDetailRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;
import util.math.VariableEWMA;

import java.util.HashMap;
import java.util.Map;

import static applications.datatype.util.VSTopologyControl.GlobalACD_STREAM_ID;
import static constants.VoIPSTREAMConstants.Conf;
import static constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);
    private static final long serialVersionUID = 5688524651591558980L;

    private VariableEWMA avgCallDuration;

    public GlobalACDBolt() {
        super(LOG, new HashMap<>() /*0.004*/);
        this.output_selectivity.put(GlobalACD_STREAM_ID, 1.0);
        this.scalable = false;
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(Field.TIMESTAMP, Field.AVERAGE);
        streams.put(GlobalACD_STREAM_ID, fields);
        return streams;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        double decayFactor = config.getDouble(Conf.ACD_DECAY_FACTOR, 86400);
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
        long timestamp = cdr.getAnswerTime().getMillis() / 1000;

        avgCallDuration.add(cdr.getCallDuration());
        collector.emit(GlobalACD_STREAM_ID, bid, new StreamValues(timestamp, avgCallDuration.getAverage()));
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;

        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            avgCallDuration.add(cdr.getCallDuration());
            collector.emit(GlobalACD_STREAM_ID, bid, new StreamValues(timestamp, avgCallDuration.getAverage()));
        }

    }
}