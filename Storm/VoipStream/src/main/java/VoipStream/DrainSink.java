package VoipStream;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import util.Log;
import util.MetricGroup;
import util.Sampler;

import java.util.Map;

public class DrainSink extends BaseRichBolt {
    private static final Logger LOG = Log.get(DrainSink.class);

    private final long samplingRate;

    private OutputCollector outputCollector;
    private Sampler latency;

    public DrainSink(long samplingRate) {
        this.samplingRate = samplingRate;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {

        // initialize
        this.outputCollector = outputCollector;
        latency = new Sampler(samplingRate);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.debug("tuple in: {}", tuple);

        long timestamp = (long) tuple.getValueByField("timestamp");
        long now = System.nanoTime();
        latency.add((now - timestamp) / 1e3, now); // microseconds

        //outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

        MetricGroup.add("latency", latency);
    }
}
