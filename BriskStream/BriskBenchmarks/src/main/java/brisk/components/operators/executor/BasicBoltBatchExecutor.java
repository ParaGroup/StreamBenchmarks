package brisk.components.operators.executor;

import brisk.components.context.TopologyContext;
import brisk.components.operators.api.AbstractBolt;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

public class BasicBoltBatchExecutor extends BoltExecutor {
    private static final long serialVersionUID = 5928745739657994175L;
    private final AbstractBolt _op;

    public BasicBoltBatchExecutor(AbstractBolt op) {
        super(op);
        _op = op;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
    }

    @Override
    public void cleanup() {
        _op.cleanup();
    }

    public void callback(int callee, Marker marker) {
        _op.callback(callee, marker);
    }


    public void execute(TransferTuple in) throws InterruptedException, BrokenBarrierException {
        _op.execute(in);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, BrokenBarrierException {
        _op.execute(in);
    }

    public void profile_execute(TransferTuple in) throws InterruptedException, BrokenBarrierException {
        _op.profile_execute(in);
    }

    public long getNumTuples() {
        return _op.getNumTuples();
    }
}
