package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import common.BloomFilter;
import common.CallDetailRecord;
import common.Constants;
import util.Log;

import java.util.Map;

class Dispatcher extends BaseRichBolt {
    private static final Logger LOG = Log.get(Dispatcher.class);

    private OutputCollector outputCollector;

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private double cycleThreshold;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;

        int approxInsertSize = Constants.VAR_DETECT_APROX_SIZE;
        double falsePostiveRate = Constants.VAR_DETECT_ERROR_RATE;
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        cycleThreshold = detector.size() / Math.sqrt(2);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("timestamp", "calling_number", "called_number", "answer_timestamp", "new_callee", "cdr", "ecr"));
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");

        // fetch values from the tuple
        CallDetailRecord cdr = (CallDetailRecord) tuple.getValueByField("cdr");
        String key = String.format("%s:%s", cdr.callingNumber, cdr.calledNumber);
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        Values values = new Values(timestamp, cdr.callingNumber, cdr.calledNumber, cdr.answerTimestamp, newCallee, cdr, -1.0);
        outputCollector.emit(values);
        LOG.debug("tuple out: {}", values);

        //outputCollector.ack(tuple);
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}
