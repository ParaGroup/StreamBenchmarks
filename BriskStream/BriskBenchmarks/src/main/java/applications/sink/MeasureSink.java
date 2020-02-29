package applications.sink;

import applications.Constants;
import applications.datatype.util.LRTopologyControl;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import helper.stable_sink_helper;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;
import brisk.util.Sampler;
import brisk.util.MetricGroup;
import applications.bolts.ysb.WinCounter;
import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import static applications.CONTROL.NUM_EVENTS;
import static applications.CONTROL.enable_latency_measurement;
import static applications.Constants.Event_Path;

public class MeasureSink extends BaseSink {
    String application = "not_defined";
    long readWords = 0;
    long readBytes = 0;
    long totalWindows = 0;
    long totalWinSize = 0;
    protected static final LinkedHashMap<Long, Long> latency_map = new LinkedHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final long serialVersionUID = 6249684803036342603L;
    protected static String directory;
    protected static String algorithm;
    protected static boolean profile = false;
    protected stable_sink_helper helper;
    protected int ccOption;
    int cnt = 0;
    private long received = 0;
    private boolean LAST = false;
    private Sampler latency_sampler;

    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
//      this.input_selectivity.put(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, 1.0);
//      this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, 1.0);
//      this.input_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, 1.0);
//      this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
    }

    public MeasureSink(String _application) {
        super(new HashMap<>());
        this.application = _application;
        this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_NOIT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID, 1.0);
//		this.input_selectivity.put(LRTopologyControl.ACCIDENTS_STREAM_ID, 1.0);
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }

    public void initialize(int task_Id_InGroup, int thisTaskId, ExecutionGraph graph) {
        super.initialize(task_Id_InGroup, thisTaskId, graph);
        int size = graph.getSink().operator.getExecutorList().size();
        ccOption = config.getInt("CCOption", 0);
        latency_sampler = new Sampler(sampling);
        String path = config.getString("metrics.output");
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , path
                , config.getDouble("predict", 0)
                , size
                , thisTaskId
                , config.getBoolean("measure", false));
//        helper.helper helper2 = new stable_sink_helper(LOG
//                , config.getInt("runtimeInSeconds")
//                , metric_path, config.getDouble("predict", 0), size, thisTaskId);
        profile = config.getBoolean("profile");
        directory = Event_Path + OsUtils.OS_wrapper("BriskStream")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getInt("num_socket"))
                + OsUtils.OS_wrapper(String.valueOf(ccOption)))
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("checkpoint")))
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("theta")));
        File file = new File(directory);
        if (!file.mkdirs()) {
        }
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
//		store = new ArrayDeque<>((int) 1E11);
        LAST = thisTaskId == graph.getSink().getExecutorID();
    }

    @Override
    public void execute(Tuple input) {
        check(cnt, input);
//        LOG.info("CNT:" + cnt);
        cnt++;
    }

    @Override
    public long getNumTuples() {
        return received;
    }

    @Override
    public void execute(TransferTuple input) {
        if (application.equals("WordCount")) {
            int bound = input.length;
            for (int i = 0; i < bound; i++) {
                char [] word = input.getCharArray(0, i);
                long count = input.getLong(1, i);
                long ts = input.getLong(2, i);
                readWords++;
                readBytes += new String(word).getBytes().length;
                received++;
                long now = System.nanoTime();
                latency_sampler.add((double)(now - ts) / 1e3, now);
                double results = helper.execute(input.getBID());
                if (results != 0) {
                    this.setResults(results);
                    if (LAST) {
                        measure_end();
                    }
                    MetricGroup.add("latency", latency_sampler);
                }
            }
        }
        else if (application.equals("YSB")) {
            int bound = input.length;
            for (int i = 0; i < bound; i++) {
                char[] cmpid = input.getCharArray(0, i);
                long count = input.getLong(1, i);
                long ts = input.getLong(2, i);
                totalWindows++;
                totalWinSize += count*3;
                received++;
                long now = System.nanoTime();
                latency_sampler.add((double)(now - ts) / 1e3, now);
                double results = helper.execute(input.getBID());
                if (results != 0) {
                    this.setResults(results);
                    if (LAST) {
                        measure_end();
                    }
                    MetricGroup.add("latency", latency_sampler);
                    WinCounter.add(totalWindows, totalWinSize);
                }
            }        
        }
        else {
            //	store.add(input);
            int bound = input.length;
            for (int i = 0; i < bound; i++) {
                boolean flag = input.getBoolean(0, i);
                long ts = input.getLong(1, i);
                received++;
                if (flag) {
                    //System.out.println("Sink riceve tupla con timestamp: " + ts);
                    long now = System.nanoTime();
                    latency_sampler.add((double)(now - ts) / 1e3, now);
                }
                double results = helper.execute(input.getBID());
                if (results != 0) {
                    this.setResults(results);
                    //LOG.info("Sink finished:" + received);
                    if (LAST) {
                        measure_end();
                    }
                    MetricGroup.add("latency", latency_sampler);
                }
            }
        }
    }

    protected void check(int cnt, Tuple input) {
        if (cnt == 0) {
            helper.StartMeasurement();
        }
        else if (cnt == (NUM_EVENTS - 1)) {
            double results = helper.EndMeasurement(cnt);
            this.setResults(results);
            LOG.info("Received:" + cnt + " throughput:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end();
            }
        }
        if (enable_latency_measurement)
            if (isSINK) {// && cnt % 1E3 == 0
                long msgId = input.getBID();
                if (msgId < max_num_msg) {
                    final long end = System.nanoTime();
                    final long start = input.getLong(1);
                    final long process_latency = end - start;//ns
                    latency_map.put(msgId, process_latency);
                }
            }
    }

    /**
     * Only one sink will do the measure_end.
     */
    protected void measure_end() {
        if (!profile) {
            for (Map.Entry<Long, Long> entry : latency_map.entrySet()) {
//                LOG.info("=====Process latency of msg====");
                //LOG.DEBUG("SpoutID:" + (int) (entry.getKey() / 1E9) + " and msgID:" + entry.getKey() % 1E9 + " is at:\t" + entry.getValue() / 1E6 + "\tms");
                latency.addValue((entry.getValue() / 1E6));
            }
            try {
//                Collections.sort(col_value);
                FileWriter f = null;
                switch (algorithm) {
                    case "random": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.latency")));
                        break;
                    }
                    case "toff": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("toff.latency")));
                        break;
                    }
                    case "roundrobin": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("roundrobin.latency")));
                        break;
                    }
                    case "worst": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.latency")));
                        break;
                    }
                    case "opt": {
                        f = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.latency")));
                        break;
                    }
                }
                Writer w = new BufferedWriter(f);
                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(String.valueOf(latency.getPercentile(percentile) + "\n"));
                }
                w.write("=======Details=======");
                w.write(latency.toString() + "\n");
                w.write("===90th===" + "\n");
                w.write(String.valueOf(latency.getPercentile(90) + "\n"));
                w.close();
                f.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            //LOG.info("Stop all threads sequentially");
//			context.stop_runningALL();
            context.Sequential_stopAll();
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
