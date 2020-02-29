package applications.bolts.vs;

import applications.bolts.comm.AbstractFilterBolt;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import model.cdr.CallDetailRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.datatypes.StreamValues;

import java.util.HashMap;
import java.util.Map;

import static applications.datatype.util.VSTopologyControl.CTBolt_STREAM_ID;
import static constants.VoIPSTREAMConstants.Field;

/**
 * Per-user total call time
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt.class);
    private static final long serialVersionUID = 2798457360174617784L;

    private double cnt = 0, cnt1 = 0, cnt2 = 0;

    public CTBolt(String configPrefix) {
        super(configPrefix, null, new HashMap<>(), 0.002);
        this.output_selectivity.put(CTBolt_STREAM_ID, 0.0);
    }

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();
        Fields fields = new Fields(Field.CALLING_NUM, Field.TIMESTAMP, outputField, Field.RECORD, outputkeyField);
        streams.put(CTBolt_STREAM_ID, fields);
        return streams;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
//        if (stat != null) stat.start_measure();
        final long bid = in.getBID();
        CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD);
        boolean newCallee = in.getBooleanByField(Field.NEW_CALLEE);
//        cnt++;
        if (cdr.isCallEstablished() && newCallee) {
            String caller = in.getStringByField(Field.CALLING_NUM);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            filter.add(caller, cdr.getCallDuration(), timestamp);
            double calltime = filter.estimateCount(caller, timestamp);
//            cnt1++;
            //LOG.DEBUG(String.format("CallTime: %f", calltime));
            collector.emit(CTBolt_STREAM_ID, bid, new StreamValues(caller, timestamp, calltime, cdr));
        }
//        double i = cnt1 / cnt;
//        if (stat != null) stat.end_measure();
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {
            CallDetailRecord cdr = (CallDetailRecord) in.getValueByField(Field.RECORD, i);
            boolean newCallee = in.getBooleanByField(Field.NEW_CALLEE, i);
//        cnt++;
            if (cdr.isCallEstablished() && newCallee) {
                String caller = in.getStringByField(Field.CALLING_NUM, i);
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                filter.add(caller, cdr.getCallDuration(), timestamp);
                double calltime = filter.estimateCount(caller, timestamp);
//            cnt1++;
                //LOG.DEBUG(String.format("CallTime: %f", calltime));
                collector.emit(CTBolt_STREAM_ID, bid, new StreamValues(caller, timestamp, calltime, cdr));
            }
        }
    }

    public void display() {
//        LOG.info("Received:" + cnt + "\tEmit:" + cnt1 + "(" + (cnt1 / cnt) + ")");
    }
}