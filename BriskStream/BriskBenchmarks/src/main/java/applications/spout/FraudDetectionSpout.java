package applications.spout;

import brisk.components.context.TopologyContext;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import constants.BaseConstants;
import helper.wrapper.StringStatesWrapper;
import brisk.execution.runtime.tuple.impl.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;
import helper.parser.Parser;
import java.io.*;
import parser.TransactionParser;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Scanner;
import constants.FraudDetectionConstants.Field;

public class FraudDetectionSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected ArrayList<char[]> array;
    protected int element = 0;
    protected int counter = 0;
    //	String[] array_array;
    char[][] array_array;
    private transient BufferedWriter writer;
    private int cnt;
    private int taskId;
    final TransactionParser parser;
    private final Fields fields;
    private long sent = 0;

    public FraudDetectionSpout() {
        super(LOG);
        this.scalable = false;
        this.parser = new TransactionParser();
        this.fields = new Fields(Field.ENTITY_ID, Field.RECORD_DATA, Field.TIMESTAMP);
    }

    public FraudDetectionSpout(Parser _parser, Fields _fields) {
        super(LOG);
        this.scalable = false;
        this.parser = (TransactionParser) _parser;
        this.fields = _fields;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        }
        else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long start = System.nanoTime();
        cnt = 0;
        counter = 0;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        // numTasks = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS));
        String OS_prefix = null;
        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        }
        else {
            OS_prefix = "unix.";
        }
        String s = System.getProperty("user.home").concat("/StreamBenchmarks/Datasets/FD/credit-card.dat");
        array = new ArrayList<>();
        try {
            openFile(s);
        }
        catch (FileNotFoundException e) {
             e.printStackTrace();
        }
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        //LOG.info("JVM PID  = " + pid);
        int end_index = array_array.length * config.getInt("count_number", 1);
        //LOG.info("spout:" + this.taskId + " elements:" + end_index);
        long end = System.nanoTime();
        //LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }

    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {}

    private void build(Scanner scanner) {
        cnt = 100;
        if (config.getInt("batch") == -1) {
            while (scanner.hasNext()) {
                array.add(scanner.next().toCharArray());//for micro-benchmark only
            }
        }
        else {
            if (!config.getBoolean("microbenchmark")) {//normal case..
                //&& cnt-- > 0
                if (OsUtils.isMac()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        array.add(scanner.nextLine().toCharArray());
                    }
                }
                else {
                    while (scanner.hasNextLine()) {
                        array.add(scanner.nextLine().toCharArray()); //normal..
                    }
                }
            }
            else {
                int tuple_size = config.getInt("size_tuple");
                LOG.info("Additional tuple size to emit:" + tuple_size);
                StringStatesWrapper wrapper = new StringStatesWrapper(tuple_size);
//                        (StateWrapper<List<StreamValues>>) ClassLoaderUtils.newInstance(parserClass, "wrapper", LOG, tuple_size);
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        construction(scanner, wrapper);
                    }
                }
                else {
                    while (scanner.hasNextLine()) {
                        construction(scanner, wrapper);
                    }
                }
            }
        }
        scanner.close();
    }

    private void construction(Scanner scanner, StringStatesWrapper wrapper) {
        String splitregex = ",";
        String[] words = scanner.nextLine().split(splitregex);
        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            sb.append(word).append(wrapper.getTuple_states()).append(splitregex);
        }
        array.add(sb.toString().toCharArray());
    }

    private void read(String prefix, int i, String postfix) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File((prefix + i) + "." + postfix), "UTF-8");
        build(scanner);
    }

    private void splitRead(String fileName) throws FileNotFoundException {
        int numSpout = this.getContext().getComponent(taskId).getNumTasks();
        int range = 10 / numSpout;//original file is split into 10 sub-files.
        int offset = this.taskId * range + 1;
        String[] split = fileName.split("\\.");
        for (int i = offset; i < offset + range; i++) {
            read(split[0], i, split[1]);
        }
        if (this.taskId == numSpout - 1) {//if this is the last executor of spout
            for (int i = offset + range; i <= 10; i++) {
                read(split[0], i, split[1]);
            }
        }
    }

    private void openFile(String fileName) throws FileNotFoundException {
        boolean split;
        split = !OsUtils.isMac() && config.getBoolean("split", true);
        if (split) {
            splitRead(fileName);
        }
        else {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            build(scanner);
        }
        array_array = array.toArray(new char[array.size()][]);
        counter = 0;
    }

    private void spout_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        //LOG.info("JVM PID  = " + pid);
        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("spout_threadId.txt")));
            writer = new BufferedWriter(fw);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.relax_reset();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {
        char[] line = array_array[counter];
        Object[] emit = parser.parse(line);
        //emit[0] = (new String("Pippo").toCharArray());
        Object[] emit2 = new Object[emit.length+1];
        for (int j=0; j<emit.length; j++)
            emit2[j] = emit[j];
        emit2[emit.length] = System.nanoTime();
        collector.emit(0, emit2); // fields: ENTITY_ID(char[]), RECORD(char[]), TIMESTAMP(long)
        counter++;
        sent++;
        if (counter == array_array.length) {
            counter = 0;
        }
        if (gen_rate != 0) { // not full speed
            long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
            active_delay(delay_nsec);
        }
    }

    @Override
    public void nextTuple_nonblocking() throws InterruptedException {
        nextTuple();
    }

    @Override
    public long getNumTuples() {
        return sent;
    }

    public void display() {
        //LOG.info("timestamp_counter:" + counter);
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }
}
