package applications;

import applications.sink.MeasureSink;
import java.util.Arrays;
import applications.topology.*;
import applications.topology.latency.LinearRoad_latency;
import applications.topology.latency.WordCount_latency;
import brisk.components.Topology;
import brisk.components.TopologyComponent;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.executorThread;
import brisk.topology.TopologySubmitter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import constants.*;
import machine.HP_Machine;
import machine.HUAWEI_Machine;
import machine.Platform;
import machine.RTM_Machine;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.Constants;
import util.OsUtils;
import applications.bolts.ysb.WinCounter;
import java.io.*;
import java.util.Collection;
import java.util.Properties;
import brisk.util.MetricGroup;
import static applications.Constants.System_Plan_Path;
import static constants.SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS;
import applications.spout.MBCounter;

class Metrics {
    public long sourceGenerated;
    public long sinkReceived;

    public Metrics(long _sG, long _sR) {
        sourceGenerated = _sG;
        sinkReceived = _sR;
    }
}

public class BriskRunner extends abstractRunner {
    private static final Logger LOG = LoggerFactory.getLogger(BriskRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private Platform p;

    private BriskRunner() {
        driver = new AppDriver();
        driver.addApp("YSB", YSB.class);
        driver.addApp("StreamingAnalysis", StreamingAnalysis.class);//Extra
        driver.addApp("WordCount", WordCount.class);
        driver.addApp("FraudDetection", FraudDetection.class);
        driver.addApp("SpikeDetection", SpikeDetection.class);
        driver.addApp("TrafficMonitoring", TrafficMonitoring.class);
        driver.addApp("LogProcessing", LogProcessing.class);
        driver.addApp("VoIPSTREAM", VoIPSTREAM.class);
        driver.addApp("LinearRoad", LinearRoad.class);//
        //test latency
        driver.addApp("WordCount_latency", WordCount_latency.class);
        driver.addApp("LinearRoad_latency", LinearRoad_latency.class);//
    }

    public static void main(String[] args) {
        BriskRunner runner = new BriskRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        }
        catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        try {
            runner.run();
        }
        catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
        }
    }

    private static Metrics runTopologyLocally(Topology topology, Configuration conf) throws InterruptedException {
        TopologySubmitter submitter = new TopologySubmitter();
        final_topology = submitter.submitTopology(topology, conf);
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();
        long start = System.currentTimeMillis();
        sinkThread.join((long) (30 * 1E3 * 60));//wait for sink thread to stop. Maximally wait for 10 mins
        long time_elapsed = (long) ((System.currentTimeMillis() - start) / 1E3 / 60);//in mins
        if (time_elapsed > 20) {
            LOG.info("Program error, exist...");
            System.exit(-1);
        }
        if (conf.getBoolean("simulation")) {
            System.exit(0);
        }
        submitter.getOM().join();
        submitter.getOM().getEM().exist();
        if (sinkThread.running) {
            LOG.info("The application fails to stop normally, exist...");
            return null;
        }
        else {
            // acquire the statistics on the number of generated tuples by the Spouts
            TopologyComponent spouts = submitter.getOM().g.getSpout().operator;
            long generated = 0;
            for (ExecutionNode e : spouts.getExecutorList()) {
                generated += e.op.getNumTuples();
            }
            // acquire the statistics on the number of received tuples by the Sinks
            TopologyComponent sinks = submitter.getOM().g.getSink().operator;
            long received = 0;
            for (ExecutionNode e : sinks.getExecutorList()) {
                received += e.op.getNumTuples();
            }
            return new Metrics(generated, received);
        }
    }

    private void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default
        // configuration
        // load default configuration
        if (configStr == null) {
            String cfg = String.format(CFG_PATH, application);
            Properties p = null;
            try {
                p = loadProperties(cfg);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            config.putAll(Configuration.fromProperties(p));
            if (mode.equalsIgnoreCase(RUN_REMOTE)) {
                final String spout_class = String.valueOf(config.get("mb.spout.class"));
                if (spout_class.equals("applications.spout.LocalStateSpout")) {
                    LOG.info("Please use kafkaSpout in cluster mode!!!");
                    System.exit(-1);
                }
            }
            config.put(Configuration.TOPOLOGY_WORKER_CHILDOPTS, CHILDOPTS);
            configuration(config);
            switch (config.getInt("machine")) {
                case 0:
                    this.p = new HUAWEI_Machine();
                    break;
                case 1:
                    this.p = new HP_Machine();
                    break;
                case 2:
                    this.p = new HP_Machine();
                    break;
                default:
                    this.p = new RTM_Machine();
            }
            if (simulation) {
                LOG.info("Simulation: use machine:" + config.getInt("machine")
                        + " with sockets:" + config.getInt("num_socket")
                        + " and cores:" + config.getInt("num_cpu"));
            }
            //configure database.
            int max_hz = 0;
            boolean profile = config.getBoolean("profile");
            //  boolean benchmark = config.getBoolean("benchmark");
            //configure threads.
            int tthread = config.getInt("tthread");
            config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);
            config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);
            config.put(BaseConstants.BaseConf.PARSER_THREADS, pthread);
            //set total parallelism, equally parallelism
            switch (application) {
                case "FraudDetection": {
                    if (profile) {
                        int threads = (int) Math.floor(tthread);
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads);
                    }
                    else {
                        int[] par_degs = Arrays.stream(parallelism_string.split(",")).mapToInt(Integer::parseInt).toArray();
                        if (par_degs.length != 3) {
                            LOG.info("Error in parsing the input arguments");
                            System.exit(1);
                        }
                        config.put(BaseConstants.BaseConf.SPOUT_THREADS, par_degs[0]);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, par_degs[0]);
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, par_degs[1]);
                        config.put(BaseConstants.BaseConf.SINK_THREADS, par_degs[2]);
                        config.put(BaseConstants.BaseConf.GEN_RATE, gen_rate);
                        config.put(BaseConstants.BaseConf.SAMPLING, sampling);
                    }
                    max_hz = FraudDetectionConstants.max_hz;
                    break;
                }
                case "SpikeDetection": {
                    if (profile) {
                        int threads = (int) Math.floor(tthread);
                        config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, threads);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads);
                    }
                    else {
                        int[] par_degs = Arrays.stream(parallelism_string.split(",")).mapToInt(Integer::parseInt).toArray();
                        if (par_degs.length != 4) {
                            LOG.info("Error in parsing the input arguments");
                            System.exit(1);
                        }
                        config.put(BaseConstants.BaseConf.SPOUT_THREADS, par_degs[0]);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, par_degs[0]);
                        config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, par_degs[1]);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, par_degs[2]);
                        config.put(BaseConstants.BaseConf.SINK_THREADS, par_degs[3]);
                        config.put(BaseConstants.BaseConf.GEN_RATE, gen_rate);
                        config.put(BaseConstants.BaseConf.SAMPLING, sampling);
                    }
                    max_hz = SpikeDetectionConstants.max_hz;
                    break;
                }
                case "TrafficMonitoring": {
                    if (profile) {
                        int threads = (int) Math.floor(tthread);
                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, threads);
                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, threads);
                    }
                    else {
                        int[] par_degs = Arrays.stream(parallelism_string.split(",")).mapToInt(Integer::parseInt).toArray();
                        if (par_degs.length != 4) {
                            LOG.info("Error in parsing the input arguments");
                            System.exit(1);
                        }
                        config.put(BaseConstants.BaseConf.SPOUT_THREADS, par_degs[0]);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, par_degs[0]);
                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, par_degs[1]);
                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, par_degs[2]);
                        config.put(BaseConstants.BaseConf.SINK_THREADS, par_degs[3]);
                        config.put(BaseConstants.BaseConf.GEN_RATE, gen_rate);
                        config.put(BaseConstants.BaseConf.SAMPLING, sampling);
                    }
                    max_hz = TrafficMonitoringConstants.max_hz;
                    break;
                }
                case "WordCount": {
                    if (profile) {
                        int threads = tthread;
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, threads);
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, threads);
                    }
                    else {
                        int[] par_degs = Arrays.stream(parallelism_string.split(",")).mapToInt(Integer::parseInt).toArray();
                        if (par_degs.length != 4) {
                            LOG.info("Error in parsing the input arguments");
                            System.exit(1);
                        }
                        config.put(BaseConstants.BaseConf.SPOUT_THREADS, par_degs[0]);
                        config.put(BaseConstants.BaseConf.PARSER_THREADS, par_degs[0]);
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, par_degs[1]);
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, par_degs[2]);
                        config.put(BaseConstants.BaseConf.SINK_THREADS, par_degs[3]);
                        config.put(BaseConstants.BaseConf.GEN_RATE, gen_rate);
                        config.put(BaseConstants.BaseConf.SAMPLING, sampling);
                    }
                    max_hz = WordCountConstants.max_hz;
                    break;
                }
                case "YSB": {
                    runtimeInSeconds = runtimeInSeconds / 10; // otherwise the execution is too long with YSB
                    if (profile) {
                        int threads = tthread;
                        config.put(YSBConstants.Conf.FILTER_THREADS, threads);
                        config.put(YSBConstants.Conf.JOINER_THREADS, threads);
                        config.put(YSBConstants.Conf.AGGREGATE_THREADS, threads);
                    }
                    else {
                        int[] par_degs = Arrays.stream(parallelism_string.split(",")).mapToInt(Integer::parseInt).toArray();
                        if (par_degs.length != 5) {
                            LOG.info("Error in parsing the input arguments");
                            System.exit(1);
                        }
                        config.put(BaseConstants.BaseConf.SPOUT_THREADS, par_degs[0]);
                        config.put(YSBConstants.Conf.FILTER_THREADS, par_degs[1]);
                        config.put(YSBConstants.Conf.JOINER_THREADS, par_degs[2]);
                        config.put(YSBConstants.Conf.AGGREGATE_THREADS, par_degs[3]);
                        config.put(BaseConstants.BaseConf.SINK_THREADS, par_degs[4]);
                        config.put(BaseConstants.BaseConf.GEN_RATE, gen_rate);
                        config.put(BaseConstants.BaseConf.SAMPLING, sampling);
                    }
                    max_hz = WordCountConstants.max_hz;
                    break;
                }
                case "LogProcessing": {
                    config.put(BaseConstants.BaseConf.SPOUT_THREADS, 1);//special treatment to LG>
                    int threads = (int) Math.floor(tthread / 5.0);
                    LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, threads);//2
                    config.put(LogProcessingConstants.Conf.GEO_STATS_THREADS, threads);//insignificant
                    config.put(LogProcessingConstants.Conf.STATUS_COUNTER_THREADS, threads);//insignificant
                    config.put(LogProcessingConstants.Conf.VOLUME_COUNTER_THREADS, threads);//insignificant
                    break;
                }
                case "VoIPSTREAM": {
                    int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 11.0));
                    LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.RCR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.ECR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.ENCR_THREADS, threads);//insignificant
                    config.put(VoIPSTREAMConstants.Conf.CT24_THREADS, threads);//insignificant
                    config.put(VoIPSTREAMConstants.Conf.ECR24_THREADS, threads);
                    //   config.put(VoIPSTREAMConstants.Conf.GLOBAL_ACD, threads); 1
                    config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, threads);//2
                    config.put(VoIPSTREAMConstants.Conf.URL_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.ACD_THREADS, threads);
                    config.put(VoIPSTREAMConstants.Conf.SCORER_THREADS, threads);
                    break;
                }
                case "LinearRoad": {
                    int threads = Math.max(1, (int) Math.floor((tthread - sthread - sithread) / 10.0));
                    //LOG.info("Average threads:" + threads);
                    config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                    config.put(LinearRoadConstants.Conf.DispatcherBoltThreads, threads);
                    config.put(LinearRoadConstants.Conf.AccidentDetectionBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.COUNT_VEHICLES_Threads, threads);//insignificant
                    //config.put(LinearRoadConstants.Conf.dailyExpBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.AccidentNotificationBoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_cv_BoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_las_BoltThreads, threads);//insignificant
                    config.put(LinearRoadConstants.Conf.toll_pos_BoltThreads, threads);//insignificant
                    //config.put(LinearRoadConstants.Conf.AccountBalanceBoltThreads, threads);
                    config.put(LinearRoadConstants.Conf.AverageSpeedThreads, threads);
                    config.put(LinearRoadConstants.Conf.LatestAverageVelocityThreads, threads);
                    break;
                }
            }
            Constants.default_sourceRate = config.getInt("targetHz");
        }
        else {
            config.putAll(Configuration.fromStr(configStr));
        }
        DescriptiveStatistics record = new DescriptiveStatistics();
        System.gc();
        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }
        // In case topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }
        // Get the topology
        Topology topology = app.getTopology(topologyName, config);
        topology.addMachine(p);
        // Run the topology
        LOG.info("Submitting topology");
        long start_time = System.nanoTime();
        Metrics result = runTopologyLocally(topology, config);
        long end_time = System.nanoTime();
        double elapsed_time_sec = (((double) (end_time - start_time)) / 1000000000L);
        LOG.info("Exiting");
        Collection<TopologyComponent> topologyComponents = final_topology.getRecords().values();
        if (application.equals("WordCount")) {
            double throughput = result.sourceGenerated/elapsed_time_sec;
            double opt_throughput = result.sourceGenerated/60;
            double mb_throughput = (MBCounter.getReadBytes() / (double) 1048576) / elapsed_time_sec;
            double opt_mb_throughput = (MBCounter.getReadBytes() / (double) 1048576) / 60;
            double sink_throughput = result.sinkReceived/elapsed_time_sec;
            LOG.info("Measured throughput: " + (int) throughput + " lines/second, " + mb_throughput + " MB/s ");
            LOG.info("Optimistic throughput: " + (int) opt_throughput + " lines/second, " + opt_mb_throughput + " MB/s ");
            LOG.info("Sink throughput: " + sink_throughput + " words/second");
        }
        else if (application.equals("TrafficMonitoring")) {
            LOG.info("Measured throughput: " + (int) (result.sinkReceived/elapsed_time_sec) + " tuples/second");
            LOG.info("Optimistic throughput: " + (int) (result.sinkReceived/60) + " tuples/second");          
        }
        else {
            LOG.info("Measured throughput: " + (int) (result.sourceGenerated/elapsed_time_sec) + " tuples/second");
            LOG.info("Optimistic throughput: " + (int) (result.sourceGenerated/60) + " tuples/second");
        }
        LOG.info("Dumping metrics");
        try {
            MetricGroup.dumpAll();
        }
        catch(Exception e) {

        }
        String algorithm;
        if (config.getBoolean("random", false)) {
            algorithm = "random";
        }
        else if (config.getBoolean("toff", false)) {
            algorithm = "toff";
        }
        else if (config.getBoolean("roundrobin", false)) {
            algorithm = "roundrobin";
        }
        else if (config.getBoolean("worst", false)) {
            algorithm = "worst";
        }
        else {
            algorithm = "opt";
        }
        String directory = System_Plan_Path + OsUtils.OS_wrapper("BriskStream")
                + OsUtils.OS_wrapper(topology.getPrefix())
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket")));
        File file = new File(directory);
        if (!file.mkdirs()) {}
        FileWriter f = null;
        try {
            switch (algorithm) {
                case "random": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.throughput")));
                    break;
                }
                case "toff": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("toff.throughput")));
                    break;
                }
                case "roundrobin": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("roundrobin.throughput")));
                    break;
                }
                case "worst": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.throughput")));
                    break;
                }
                case "opt": {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.throughput")));
                    break;
                }
            }
            Writer w = new BufferedWriter(f);
            w.write("Bounded throughput (k events/s):" + config.getDouble("bound", 0) + "\n");
            w.write("predict throughput (k events/s):" + config.getDouble("predict", 0) + "\n");
            w.write("finished measurement (k events/s):" + record.getPercentile(50) + "("
                    + (record.getPercentile(50) / config.getDouble("predict", 0)) + ")" + "\n");
            w.close();
            f.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
