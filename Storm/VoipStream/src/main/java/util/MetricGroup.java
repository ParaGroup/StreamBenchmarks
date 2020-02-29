package util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Stack;

// TODO instead of a static class one could pass the object around
public class MetricGroup {
    private static HashMap<String, Stack<Sampler>> map;

    static {
        map = new HashMap<>();
    }

    // XXX this is not time critical, making the whole method synchronized is good enough
    public static synchronized void add(String name, Sampler sampler) {
        Stack<Sampler> samplers = map.computeIfAbsent(name, key -> new Stack<>());
        samplers.add(sampler);
    }

    // XXX this consumes the groups
    public static void dumpAll() throws IOException {
        for (String name : map.keySet()) {
            Metric metric = getMetric(name);
            metric.dump();
        }
    }

    private static Metric getMetric(String name) {
        Metric metric = new Metric(name);

        // consume all the groups
        long total = 0;
        Stack<Sampler> samplers = map.get(name);
        while (!samplers.empty()) {
            Sampler sampler = samplers.pop();

            // add all the values from the sampler
            total += sampler.getTotal();
            for (double value : sampler.getValues()) {
                metric.add(value);
            }
        }

        // set total
        metric.setTotal(total);
        return metric;
    }
}
