package LinearRoad;

import org.apache.storm.topology.TopologyBuilder;
import util.Configuration;
import util.Log;
import org.slf4j.Logger;
import java.io.IOException;

public class LinearRoad {
    private static final Logger LOG = Log.get(LinearRoad.class);

    // Main
    public static void main(String[] args) throws IOException {
        Configuration configuration = Configuration.fromArgs(args);
        String datasetPath = configuration.getTree().get("dataset").textValue();
        long runTime = configuration.getTree().get("run_time").numberValue().longValue();
        long samplingRate = configuration.getTree().get("sampling_rate").numberValue().longValue();
        long gen_rate = configuration.getTree().get("gen_rate").numberValue().longValue();
        // print app info
        LOG.info("Executing LinearRoad with parameters:\n" +
                 "  * rate: " + ((gen_rate == 0) ? "full_speed" : gen_rate) + " tuples/second\n" +
                 "  * sampling: " + samplingRate + "\n" +
                 "  * topology: complex with 9 operators");

        // prepare the topology
        TopologyBuilder topologyBuilder = new TopologyBuilderHints(configuration);
        topologyBuilder.setSpout("source", new LineReaderSpout(runTime, gen_rate, datasetPath));
        topologyBuilder.setBolt("dispatcher", new Dispatcher()).shuffleGrouping("source");

        topologyBuilder.setBolt("average_speed", new AverageSpeed()).shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("last_average_speed", new LastAverageSpeed()).shuffleGrouping("average_speed");
        topologyBuilder.setBolt("toll_notification_las", new TollNotificationLas()).shuffleGrouping("last_average_speed");

        topologyBuilder.setBolt("count_vehicles", new CountVehicles()).shuffleGrouping("dispatcher");
        topologyBuilder.setBolt("toll_notification_cv", new TollNotificationCv()).shuffleGrouping("count_vehicles");

        topologyBuilder.setBolt("toll_notification_pos", new TollNotificationPos()).shuffleGrouping("dispatcher");

        topologyBuilder.setBolt("sink", new DrainSink(samplingRate))
                .shuffleGrouping("toll_notification_las")
                .shuffleGrouping("toll_notification_cv")
                .shuffleGrouping("toll_notification_pos");

        // start!
        Topology.submit(topologyBuilder, configuration);
    }
}
