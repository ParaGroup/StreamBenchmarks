package applications.bolts.sd;

import applications.Constants;
import brisk.components.operators.base.filterBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.SpikeDetectionConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import java.util.HashMap;

/**
 * Emits a tuple if the current value_list surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class SpikeDetectionBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionBolt.class);
    private static final long serialVersionUID = -5919724558309333175L;
    double cnt = 0;
    double cnt1 = 0;
    int loop = 1;
    private double spikeThreshold;

    public SpikeDetectionBolt() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
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
        spikeThreshold = config.getDouble(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THRESHOLD, 0.025d);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // not in use
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bounds = in.length;
        for (int i = 0; i < bounds; i++) {
            double movingAverageInstant = in.getDouble(1, i);
            double nextDouble = in.getDouble(2, i);
            long ts = in.getLong(3, i);
            if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
                collector.emit(0, true, ts);
            }
            else {
                //collector.emit(0, false, ts);
            }
        }
    }

    @Override
    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bounds = in.length;
        for (int i = 0; i < bounds; i++) {
            double movingAverageInstant = in.getDouble(1, i);
            double nextDouble = in.getDouble(2, i);
            long ts = in.getLong(3, i);
            if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
                collector.emit(0, true, ts);
            }
            else {
                //collector.emit(0, false, ts);
            }
        }
    }

    public void display() {
        //LOG.info(this.getContext().getThisTaskId() + " cnt:" + cnt + "\tcnt1:" + cnt1 + "\toutput selectivity:" + ((cnt1) / cnt));
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(SpikeDetectionConstants.Field.FLAG, SpikeDetectionConstants.Field.TIMESTAMP);
    }
}
