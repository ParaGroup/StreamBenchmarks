package FraudDetection;

import Util.Log;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.util.Collector;
import MarkovModelPrediction.Prediction;
import Constants.BaseConstants.BaseField;
import Constants.FraudDetectionConstants.*;
import org.apache.commons.lang.StringUtils;
import MarkovModelPrediction.ModelBasedPredictor;
import MarkovModelPrediction.MarkovModelPredictor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

/** 
 *  @author  Gabriele Mencagli
 *  @version August 2019
 *  
 *  The operator is in charge of implementing outliers detection.
 *  Given a transaction sequence of a customer, there is a probability associated with each path
 *  of state transition which indicates the chances of fraudolent activities. Only tuples for
 *  which an outlier has been identified are sent out.
 */ 
public class FraudPredictor extends RichFlatMapFunction<Source_Event, Output_Event> {
    private static final Logger LOG = Log.get(FraudPredictor.class);
    private ModelBasedPredictor predictor;
    private long t_start;
    private long t_end;
    private long processed;
    private long outliers;
    private int par_deg;
    protected Configuration config;
    private long exec_time_ns = 0;

    // Constructor
    public FraudPredictor(Configuration _config) {
        config = _config;
    }

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        outliers = 0;                // total number of outliers
        String strategy = config.getString(Conf.PREDICTOR_MODEL, "mm");
        if (strategy.equals("mm")) {
            //LOG.debug("[Predictor] creating Markov Model Predictor");
            predictor = new MarkovModelPredictor(config);
        }
    }

    // flatmap method
    @Override
    public void flatMap(Source_Event input, Collector<Output_Event> output) {
        String entityID = input.entityID;
        String record = input.transaction;
        long timestamp = input.ts;
        Prediction p = predictor.execute(entityID, record);
        //LOG.debug("[Predictor] tuple: entityID " + entityID + ", record " + record + ", ts " + timestamp);
        // send outliers
        if (p.isOutlier()) {
            outliers++;
            output.collect(new Output_Event(entityID, p.getScore(), timestamp));
            //LOG.debug("[Predictor] outlier: entityID " + entityID + ", score " + p.getScore());
        }
        processed++;
        t_end = System.nanoTime();
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        /*LOG.info("[Predictor] execution time: " + t_elapsed +
                           " ms, processed: " + processed +
                           ", outliers: " + outliers +
                           ", bandwidth: " + processed / (t_elapsed / 1000) +  " tuples/s");*/
    }
}
