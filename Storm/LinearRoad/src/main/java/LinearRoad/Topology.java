package LinearRoad;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import util.Configuration;
import util.Log;
import util.MetricGroup;

public class Topology {
    private static final Logger LOG = Log.get(Topology.class);

    public static final String TOPOLOGY_NAME = "LinearRoad";

    private final static long POLLING_TIME_MS = 1000;
    private final static int BUFFER_SIZE = 32768; // XXX explicit default Storm value

    public static void submit(TopologyBuilder topologyBuilder, Configuration configuration) {
        // build the topology
        StormTopology topology = topologyBuilder.createTopology();

        // set the buffer size to avoid excessive buffering at the spout
        Config config = new Config();
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, BUFFER_SIZE);

        // flush as soon as possible in throttled mode (minimize the latency)
        if (configuration.getTree().get("gen_rate").numberValue().longValue() != 0) {
            config.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 1);
            config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);
        }

        // submit it to storm
        try {
            LOG.info("Submitting topology");

            // submit the topology to a local cluster
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, topology);

            // wait for termination
            LOG.info("Waiting for topology termination...");
            while (cluster.getNimbus().getClusterInfo().get_topologies_size() > 0) {
                Thread.sleep(POLLING_TIME_MS);
            }

            // kill cluster
            LOG.info("...Shutting down cluster");
            cluster.shutdown();
            LOG.info("Exiting");
            // dump the metrics
            LOG.info("Dumping metrics");
            MetricGroup.dumpAll();

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        // XXX force exit because the JVM may hang waiting for a dangling
        // reference...
        System.exit(0);
    }
}
