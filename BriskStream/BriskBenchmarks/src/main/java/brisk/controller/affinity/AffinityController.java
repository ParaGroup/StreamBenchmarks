package brisk.controller.affinity;

import machine.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;
import util.OsUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by I309939 on 11/8/2016.
 */
public class AffinityController {
    private final static Logger LOG = LoggerFactory.getLogger(AffinityController.class);
    private final Configuration conf;
    //(socket, core pointer)
    private final ArrayList<Integer>[] mapping_node;
    private final Map<Integer, Integer> cpu_pnt = new HashMap<>();
    private final int cores;
    private final int sockets;
    int offset = 0;

    public AffinityController(Configuration conf, Platform p) {
        this.conf = conf;
        //        boolean simulation = conf.getBoolean("simulation", true);
//        if (simulation) {
//            this.cores = 8;
//            this.num_socket = 8;
//        } else {
//            this.num_socket = this.p.num_socket;
//            this.cores = numCPUs() / num_socket;
//        }
        OsUtils.configLOG(LOG);
        this.sockets = conf.getInt("num_socket", 8);
        this.cores = conf.getInt("num_cpu", 35);
        mapping_node = Platform.getNodes(conf.getInt("machine"));
//		Integer[] sockets_usage = new Integer[8];

        cpu_pnt.put(0, 1);
        for (int i = 1; i < 8; i++) {
            cpu_pnt.put(i, 0);
//			sockets_usage[i] = 0;
        }
        //LOG.info("NUM of Sockets:" + sockets + "\tNUM of Cores:" + cores);
    }

    //depends on the server..

    public void clear() {
        cpu_pnt.put(0, 1);
        for (int i = 1; i < 8; i++) {
            cpu_pnt.put(i, 0);
        }
    }

    /**
     * one thread one socket mapping..
     *
     * @param node
     * @return
     */
    public long[] require(int node) {

        if (node != -1) {
            return requirePerSocket(node);
//            sockets_usage[node]++;
//            if (sockets_usage[node] > cores) {
//                return requirePerSocket(node);
//            } else
//                return requirePerCore(node);
        }
        return requirePerSocket(node);
    }

    private synchronized long[] requirePerSocket(int node) {

        if (node == -1) {//run as native execution
            return require();
        } else {

            int num_cpu = conf.getInt("num_cpu", 8);
            LOG.info("num_cpu:" + num_cpu);
            if (node >= sockets) {
                node = sockets - 1;//make sure less than maximum num_socket.
            }

            long[] cpus = new long[num_cpu];
            for (int i = 0; i < num_cpu; i++) {
                int cnt = cpu_pnt.get(node) % num_cpu;//make sure less than configurable cpus per socket.
                try {
//                    LOG.info("cnt:" + cnt);
                    ArrayList<Integer> list = this.mapping_node[node];
//                    LOG.info("cores on this node:" + list);
                    cpus[i] = (list.get(cnt));
                } catch (Exception e) {
                    LOG.info("Not available CPUs...?" + e.getMessage());
                    System.exit(-1);
                }
                cpu_pnt.put(node, (cnt + 1) % cores);

            }

            return cpus;

        }
    }

    /**
     * one thread one core mapping.
     *
     * @param node
     * @return
     */
    public synchronized long[] requirePerCore(int node) {

        node += offset;//pick cores from next nodes.

        if (node == -1) {//run as native execution
            return require();

        } else {

//			int num_cpu = conf.getInt("num_cpu", 8);

//			if (node >= sockets) {
//				node = sockets - 1;//make sure less than maximum num_socket.
//			}

            long[] cpus = new long[1];

            //LOG.DEBUG("request a core from Node:" + node);
            int cnt = cpu_pnt.get(node);

            try {
                cpus[0] = (this.mapping_node[node].get(cnt));
            } catch (Exception e) {
                LOG.info("No available CPUs");
                System.exit(-1);
            }

//			if (node == 0 && (cnt + 1) % cores == 0) {
//				cpu_pnt.put(node, 1);//CPU 0 cannot be used.
//			} else {
//				cpu_pnt.put(node, (cnt + 1));//select the next core at next time.
//			}

//			LOG.info("cnt:" + cnt + " pick CPU" + cpus[0] + " from nodes: " + node);
            if ((conf.getBoolean("profile", false) || conf.getBoolean("NAV", false)) && cnt == 17) {
                offset++;
//				LOG.info("offset ++");
            } else {
                cpu_pnt.put(node, (cnt + 1));//select the next core at next time.
            }
            //LOG.DEBUG("Got CPU:" + Arrays.toString(cpus));
            return cpus;
        }
    }


    //depends on the server..
    private long[] require() {

        int num_node = conf.getInt("num_socket", 1);
        int num_cpu = conf.getInt("num_cpu", 1);

        long[] cpus = new long[num_cpu * num_node];


        for (int j = 0; j < num_node; j++) {
            for (int i = num_cpu * j; i < num_cpu * j + num_cpu; i++) {
                int cnt = cpu_pnt.get(j) % num_cpu;//make sure less than configurable CPU per socket.
                cpus[i] = (this.mapping_node[j].get(cnt));
                cpu_pnt.put(j, (cnt + 1) % cores);

            }
        }

        return cpus;

    }


}
