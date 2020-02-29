package LinearRoad;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.storm.topology.*;
import org.slf4j.Logger;
import util.Configuration;
import util.Log;

public class TopologyBuilderHints extends TopologyBuilder {
    private static final Logger LOG = Log.get(TopologyBuilderHints.class);

    private Configuration configuration;

    public TopologyBuilderHints(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        int parallelismHint = getParallelismHint(id);
        LOG.info("SPOUT: {} ({})", id, parallelismHint);
        return super.setSpout(id, spout, parallelismHint);
    }

    @Override
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        int parallelismHint = getParallelismHint(id);
        LOG.info("BOLT: {} ({})", id, parallelismHint);
        return super.setBolt(id, bolt, parallelismHint);
    }

    private int getParallelismHint(String id) {
        JsonNode jsonNode = configuration.getTree().get(id);
        if (jsonNode == null) {
            return 1;
        } else {
            return jsonNode.numberValue().intValue();
        }
    }
}
