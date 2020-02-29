package brisk.optimization.impl.scheduling;

import brisk.execution.ExecutionGraph;
import brisk.execution.ExecutionNode;
import brisk.optimization.impl.SchedulingPlan;
import brisk.optimization.model.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Configuration;

import java.util.ArrayList;

public class randomPlan_NoConstraints extends randomSearch_NoConstrains {
    private final static Logger LOG = LoggerFactory.getLogger(randomPlan_NoConstraints.class);

    public randomPlan_NoConstraints(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        super(graph, numNodes, numCPUs, cons, conf);
    }

    @Override
    public SchedulingPlan Search(boolean worst_plan, int timeoutMs) {
        initilize(worst_plan, conf);
        //if it is not used in set_executor_ready plan..

        LOG.info("Randomly search for a plan");
        //main course.
        final ArrayList<ExecutionNode> sort_opList = graph.sort();
        final SchedulingPlan currentplan = new SchedulingPlan(currentPlan, false);
        return Packing(currentplan, graph, sort_opList);
    }
}
