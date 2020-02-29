package brisk.queue;

import brisk.execution.ExecutionNode;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.MpscLinkedQueue8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.OsUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * Created by shuhaozhang on 11/7/16.
 * There's one PC per pair of "downstream, downstream operator".
 * PC is owned by streamController, which is owned by each executor.
 */
public class MPSCController extends QueueController {
    private static final Logger LOG = LoggerFactory.getLogger(MPSCController.class);
    private static final long serialVersionUID = 6103946447980906477L;
    private Map<Integer, Queue> outputQueue;//<Downstream executor ID, corresponding output queue>

    /**
     * This is where partition ratio is being updated.
     *
     * @param downExecutor_list
     */
    public MPSCController(HashMap<Integer, ExecutionNode> downExecutor_list) {
        super(downExecutor_list);
    }


    public boolean isEmpty() {
        for (int executor : downExecutor_list.keySet()) {
            Queue queue = outputQueue.get(executor);
            if (!queue.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Allocate memory for queue structure here.
     *
     * @param linked
     * @param desired_elements_epoch_per_core
     */
    public void allocate_queue(boolean linked, int desired_elements_epoch_per_core) {
        outputQueue = new HashMap<>();
//		PerfectHashMap.Builder<Object, Object> builder = PerfectHashMap.newBuilder();

        //= new ConcurrentHashMap();
//		serializer = new Serializer();
        for (int executor : downExecutor_list.keySet()) {
            //clean_executorInformation the queue if it exist
//			Queue queue = outputQueue.get(executor);
//			if (queue != null) {
////                if(queue instanceof P1C1Queue)
//				LOG.info("relax_reset the old queue");
//				queue.clear();
//				System.gc();
//			}
//			builder.add(executor, new MpscArrayQueue((int) Math.pow(2, 17)));


//
            if (OsUtils.isWindows() || OsUtils.isMac()) {//local debug
                outputQueue.put(executor, new MpscArrayQueue(desired_elements_epoch_per_core));
            } else {
                if (linked) {
                    outputQueue.put(executor, new MpscLinkedQueue8<>());
                } else {
//                    int queue_size = (int) (desired_elements_epoch/2);//* 1.2
//                    LOG.info("finally, set queue size to be:" + queue_size);
                    outputQueue.put(executor, new MpscArrayQueue(desired_elements_epoch_per_core));//(int) Math.pow(2, 17)= 131072 160000
                }
            }
        }
//		outputQueue = builder.build();

    }

    public Queue get_queue(int executor) {
        return outputQueue.get(executor);
    }

}

