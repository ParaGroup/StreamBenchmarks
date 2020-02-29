package applications.bolts.fd;

import brisk.components.operators.base.filterBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import constants.BaseConstants;
import constants.FraudDetectionConstants;
import model.predictor.MarkovModelPredictor;
import model.predictor.ModelBasedPredictor;
import model.predictor.Prediction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import java.util.HashMap;

public class FraudPredictorBolt extends filterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FraudPredictorBolt.class);
    private static final long serialVersionUID = 6445550040247603261L;
    double sel = 0;
    double nsel = 0;
    int cnt = 0;
    int loop = 1;
    private ModelBasedPredictor predictor;

    public FraudPredictorBolt() {
        super(LOG, new HashMap<>());
        this.read_selectivity = 3;
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 1.0);//workaround to ensure same output selectivity
        this.setStateful();
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 65;
        }
        else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL);
        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        //not in use.
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        final int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] entityID = in.getCharArray(0, i);
            char[] record = in.getCharArray(1, i);
            long ts = in.getLong(2, i);
            Prediction p = predictor.execute(entityID, record);//use fixed input to stabilize the running?
            if (p.isOutlier()) {
                collector.emit(0, p.isOutlier(), ts);
            }
        }
    }

    @Override
    public void profile_execute(TransferTuple in) throws InterruptedException {
        final int bound = in.length;
        for (int i = 0; i < bound; i++) {
            char[] entityID = in.getCharArray(0, i);
            char[] record = in.getCharArray(1, i);
            long ts = in.getLong(2, i);
            Prediction p = predictor.execute(entityID, record);//use fixed input to stabilize the running?
            if (p.isOutlier()) {
                collector.emit(0, p.isOutlier(), ts);
            }
        }
    }

    public void display() {
//		LOG.info("cnt:" + cnt + "\tcnt1:" + sel + "(" + (sel / cnt) + ")");
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(FraudDetectionConstants.Field.FLAG, FraudDetectionConstants.Field.TIMESTAMP);
    }
}
