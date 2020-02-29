package applications;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Constants;
import util.OsUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by I309939 on 7/30/2016.
 */
public abstract class abstractRunner {
    protected static final String RUN_LOCAL = "local";
    protected static final String RUN_REMOTE = "remote";
    private static final Logger LOG = LoggerFactory.getLogger(abstractRunner.class);
    protected static String CFG_PATH = null;

    @Parameter(names = {"--Fault_tolerance"}, description = "Fault_tolerance enable")
    public boolean Fault_tolerance = false;

    @Parameter(names = {"--partition"}, description = "partition database enable")
    public boolean partition = false;


    @Parameter(names = {"--transaction"}, description = "transaction enable")
    public boolean transaction = false;

    @Parameter(names = {"--measure"}, description = "measure enable")
    public boolean measure = false;


    @Parameter(names = {"--checkpoint"}, description = "checkpoint interval")
    public double checkpoint = 1;// default checkpoint interval.

    @Parameter(names = {"--NUM_ACCESS"}, description = "NUM_ACCESS per transaction")
    public int NUM_ACCESS = 10;//

    @Parameter(names = {"--NUM_ITEMS"}, description = "NUM_ITEMS in DB.")
    public int NUM_ITEMS = 100_000;//

    @Parameter(names = {"--CCOption"}, description = "CC options")
    public int CCOption = 0;//0: T-Stream, 1: OLB, 2: LWM

    @Parameter(names = {"--backPressure"}, description = "backPressure")
    public boolean backPressure = false;

    @Parameter(names = {"--common"}, description = "common shared by consumers")
    public boolean common = false;

    @Parameter(names = {"--linked"}, description = "linked")
    public boolean linked = false;

    @Parameter(names = {"--shared"}, description = "shared by multi producers")
    public boolean shared = true;

    @Parameter(names = {"--scale_factor"}, description = "scale_factor")
    public double scale_factor = 1; //<=1

    @Parameter(names = {"--ratio_of_read"}, description = "ratio_of_read")
    public double ratio_of_read = 0.5; //<=1

    @Parameter(names = {"--ratio_of_multi_partition"}, description = "ratio_of_multi_partition")
    public double ratio_of_multi_partition = 0; //<=1

    @Parameter(names = {"--number_partitions"}, description = "number_partitions")
    public int number_partitions = 6;

    @Parameter(names = {"--theta"}, description = "theta")
    public double theta = 0.6; //0.6==medium contention

    @Parameter(names = {"--gc_factor"}, description = "gc_factor")
    public double gc_factor = 1; //<=1

    @Parameter(names = {"--machine"}, description = "which machine to use? 0:NUS machine, 1: HPI machine, you may add more..")
    public int machine = 0;

    @Parameter(names = {"--plan"}, description = "benchmarking the corresponding plan")
    public int plan = 0;

    @Parameter(names = {"--benchmark"}, description = "benchmarking the throughput of all applications")
    public boolean benchmark = false;

    @Parameter(names = {"--load"}, description = "benchmarking the throughput of all applications")
    public boolean load = false;

    @Parameter(names = {"--microbenchmark"}, description = "benchmarking the throughput of all applications")
    public boolean microbenchmark = false;
    @Parameter(names = {"--worst"}, description = "worst case plan study")
    public boolean worst = false;
    @Parameter(names = {"--profile"}, description = "profiling")
    public boolean profile = false;
    @Parameter(names = {"--profile_start"}, description = "profile_start")
    public int profile_start = -1;
    @Parameter(names = {"--profile_end"}, description = "profile_end")
    public int profile_end = -1;
    @Parameter(names = {"--manual"}, description = "manual")
    public boolean manual = false;
    @Parameter(names = {"--DO"}, description = "dynamic optimization")
    public boolean DO = false;
    @Parameter(names = {"-native"}, description = "native execution")
    public boolean NAV = false;
    @Parameter(names = {"--random"}, description = "random plan")
    public boolean random = false;
    @Parameter(names = {"--roundrobin", "-RR"}, description = "roundrobin plan")
    public boolean roundrobin = false;
    @Parameter(names = {"--toff"}, description = "toff plan")
    public boolean toff = false;

    @Parameter(names = {"--percentile"}, description = "profile percentile")
    public int percentile = 50;
    @Parameter(names = {"--routing"}, description = "routing optimization")
    public boolean routing = false;
    @Parameter(names = {"--parallelism_tune", "--tune"}, description = "routing optimization")
    public boolean parallelism_tune = false;

    @Parameter(names = {"--sim", "--simulation"}, description = "simulate hardware environment")
    public boolean simulation = false;
    @Parameter(names = {"--monte"}, description = "monte carlo simulation ")
    public boolean monte = false;
    @Parameter(names = {"--num_socket"}, description = "allowed #socket")
    public int num_socket = 1;

    @Parameter(names = {"--num_cpu"}, description = "allowed #CPU cores on each socket")
    public int num_cpu = -1;

    @Parameter(names = {"--relax"}, description = "constraint relax factor")
    public double relax = 1;

    @Parameter(names = {"--repeat"}, description = "repeat execution")
    public int repeat = 1;

    @Parameter(names = {"--loop"}, description = "measure time")
    public int loop = 100000;
    @Parameter(names = {"--JVM"}, description = "JVM size in GB")
    public int JVM = 256;
    @Parameter(names = {"--THz", "-THz"}, description = "target input Hz")
    public double THz = Constants.default_sourceRate;
    @Parameter(names = {"-input"}, description = "target percentage input rate")
    public double input = 1.0;

    @Parameter(names = {"--rate"}, description = "spout generation rate")
    public int gen_rate = 0;

    @Parameter(names = {"--sampling"}, description = "sink sampling rate")
    public int sampling = 0;

    @Parameter(names = {"--size_tuple"}, description = "size_tuple (number of elements in state)")
    public int size_tuple = 0;
    @Parameter(names = {"-w", "--window"}, description = "window")
    public int window = 2;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 60;
    @Parameter(names = {"-st"}, description = "spout.threads", required = false)
    public int sthread = 1;//default 5
    @Parameter(names = {"-sit"}, description = "sink.threads", required = false)
    public int sithread = 1;//default 10
    @Parameter(names = {"-pt"}, description = "parser.threads", required = false)
    public int pthread = 1;

    @Parameter(names = {"-tt"}, description = "parallelism", required = false)
    public int tthread = -1;
    @Parameter(names = {"-ct1"}, description = "bolt.threads", required = false)
    public int threads1 = 1;
    @Parameter(names = {"-ct2"}, description = "bolt.threads", required = false)
    public int threads2 = 1;
    @Parameter(names = {"-ct3"}, description = "bolt.threads", required = false)
    public int threads3 = 1;
    @Parameter(names = {"-ct4"}, description = "bolt.threads", required = false)
    public int threads4 = 1;
    @Parameter(names = {"-ct5"}, description = "bolt.threads", required = false)
    public int threads5 = 1;

    @Parameter(names = {"--TP"}, description = "TP threads")
    public int TP = 2;// default TP threads

    @Parameter
    public List<String> parameters = Lists.newArrayList();
    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = RUN_LOCAL;
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = false)
    public String application = "WordCount"; /*"wordcount";*/ /*"streamingAnalysis"*/

    @Parameter(names = {"-t", "--Brisk.topology-name"}, required = false, description = "The name of the Brisk.topology")
    public String topologyName;
    @Parameter(names = {
            "--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = "";
    @Parameter(names = {"-bt"}, description = "fixed batch", required = false)
    public int batch = 100;

    @Parameter(names = {"--upperlimit"}, description = "Test upperlimit throughput")
    public boolean upperlimit = false;
    @Parameter(names = {"--verbose"}, description = "whether print execution detail")
    public boolean verbose = false;

    @Parameter(names = {"--task_type"}, description = "task_type, 0:stateless, 1:PS, 2: FS")
    public int task_type = 0;
    @Parameter(names = {"--I_C"}, description = "I_C")
    public int I_C = 0;
    @Parameter(names = {"--O_C"}, description = "O_C")
    public int O_C = 0;

    @Parameter(names = {"-co"}, description = "TOPOLOGY_WORKER_CHILDOPTS", required = false)
    public String CHILDOPTS = "";
    @Parameter(names = {"-tune"}, description = "tune parallelism", required = false)
    public int tune = 4;//disable tune and use 1 socket setting by default.
    @Parameter(names = {"-log"}, description = "log property", required = false)
    public String log = "standout";
    @Parameter(names = {"--timeslice"}, description = "time slice used in spout (ms)")
    public int timeSliceLengthMs = 100;//ms

    @Parameter(names = {"--parallelism_legacy"}, description = "default parallelism setting per operator used in Flink-Storm translation")
    public int parallelism = 10;

    @Parameter(names = {"--parallelism"}, description = "parallelism string")
    public String parallelism_string = "";

    @Parameter(names = {"--compressRatio"}, description = "compressRatio")
    public int compressRatio = 1;


    /**
     * config.put("runtimeInSeconds", runtimeInSeconds);
     * config.put("size_tuple",size_tuple);
     * config.put("task_type",task_type);
     * config.put("I_C",I_C);
     * config.put("O_C",O_C);
     * config.put("window",window);
     * config.put("size_state",size_state);
     */

    public abstractRunner() {
        if (OsUtils.isWindows()) {
            CFG_PATH = System.getProperty("user.home")
                    .concat("\\Documents\\briskstream\\common\\src\\main\\resources\\config\\%s.properties");
            metric_path = System.getProperty("user.home")
                    .concat("\\Documents\\briskstream\\metric_output");
        } else {
            CFG_PATH = System.getProperty("user.home")
                    .concat("/StreamBenchmarks/BriskStream/common/src/main/resources/config/%s.properties");
            metric_path = System.getProperty("user.home")
                    .concat("/StreamBenchmarks/BriskStream/metric_output");
        }
    }

    public static Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        is = new FileInputStream(filename);
        properties.load(is);
        is.close();
        return properties;
    }

    public void configuration(HashMap<String, Object> config) {
        config.put("application", application);
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("compressRatio", compressRatio);
        config.put("parallelism_legacy", parallelism);
        config.put("load", load);
        config.put("toff", toff);
        config.put("roundrobin", roundrobin);
        config.put("microbenchmark", microbenchmark);
        config.put("percentile", percentile);
        config.put("machine", machine);
        config.put("random", random);
        config.put("parallelism_tune", parallelism_tune);
        config.put("input", input);
        config.put("pthread", pthread);
        config.put("ct1", threads1);
        config.put("ct2", threads2);
        config.put("ct3", threads3);
        config.put("ct4", threads4);
        config.put("repeat", repeat);
        config.put("plan", plan);
        config.put("manual", manual);
        config.put("profile", profile);
        config.put("profile_start", profile_start);
        config.put("profile_end", profile_end);
        config.put("benchmark", benchmark);
        config.put("gc_factor", gc_factor);
        if (num_socket != -1) {
            config.put("num_socket", num_socket);
        } else {
            config.put("num_socket", OsUtils.totalSockets());
        }


        if (num_cpu != -1) {
            config.put("num_cpu", num_cpu);
        } else {
            config.put("num_cpu", OsUtils.TotalCores() / OsUtils.totalSockets());
        }
        config.put("transaction", transaction);
        config.put("Fault_tolerance", Fault_tolerance);

        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", partition);

        config.put("measure", measure);
        config.put("checkpoint", checkpoint);
        config.put("TP", TP);
        config.put("NUM_ACCESS", NUM_ACCESS);
        config.put("NUM_ITEMS", NUM_ITEMS);
        config.put("CCOption", CCOption);
        config.put("backPressure", backPressure);
        config.put("common", common);
        config.put("linked", linked);
        config.put("shared", shared);
        config.put("scale_factor", scale_factor);
        config.put("ratio_of_read", ratio_of_read);
        config.put("theta", theta);
        config.put("relax", relax);
        config.put("monte", monte);
        config.put("DO", DO);
        config.put("NAV", NAV);
        config.put("routing", routing);
        config.put("simulation", simulation);
        config.put("loop", loop);
        config.put("JVM", JVM);
        if (tthread != -1) {
            config.put("tthread", tthread);
        } else {
            config.put("tthread", 1);
        }
        // load default configuration
        config.put("targetHz", THz);
        config.put("timeSliceLengthMs", timeSliceLengthMs);

        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("metrics.output", metric_path);
        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("size_tuple", size_tuple);

        config.put("task_type", task_type);
        config.put("I_C", I_C);
        config.put("O_C", O_C);
        config.put("window", window);
//
//        config.put("sink_class", sink_class);
//        config.put("spout_class", spout_class);
//        config.put("spout_parser", parser_class);
        config.put("mode", mode);
        config.put("verbose", verbose);
        config.put("upperlimit", upperlimit);
        config.put("worst", worst);
//        LOG.info("verbose:" + verbose);
//        final String spark_output_path = metric_path + OsUtils.OS_wrapper("spark_throughput.txt");
//        LOG.info("set my.log path:" + spark_output_path);
        System.setProperty("my.log", metric_path);
    }
}
