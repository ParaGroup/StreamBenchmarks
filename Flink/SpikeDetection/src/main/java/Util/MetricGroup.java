package Util;

import java.util.Stack;
import java.util.HashMap;
import java.io.IOException;

// MetricGroup class
public class MetricGroup {
    private static HashMap<String, Stack<Sampler>> map;
    static {
        map = new HashMap<>();
    }

    // this is not time critical, making the whole method synchronized is good enough
    public static synchronized void add(String name, Sampler sampler) {
        Stack<Sampler> samplers = map.computeIfAbsent(name, key -> new Stack<>());
        samplers.add(sampler);
    }

    // this consumes the groups
    public static void dumpAll() throws IOException {
        for (String name : map.keySet()) {
            Metric metric = getMetric(name);
            metric.dump();
        }
    }

    // getMetric method
    private static Metric getMetric(String name) {
        Metric metric = new Metric(name);
        // consume all the groups
        Stack<Sampler> samplers = map.get(name);
        while (!samplers.empty()) {
            Sampler sampler = samplers.pop();
            metric.setTotal(sampler.getTotal());
            // add all the values from the sampler
            for (double value : sampler.getValues()) {
                metric.add(value);
            }
        }
        return metric;
    }
}
