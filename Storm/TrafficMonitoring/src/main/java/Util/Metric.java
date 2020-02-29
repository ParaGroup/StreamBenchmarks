package Util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

// Metric class
public class Metric implements Serializable {
    private String name;
    private String fileName;
    private DescriptiveStatistics descriptiveStatistics;
    private long total;

    // constructor
    public Metric(String name) {
        this.name = name;
        fileName = String.format("metric_%s.json", name);
        descriptiveStatistics = new DescriptiveStatistics();
    }

    // add method
    public void add(double value) {
        descriptiveStatistics.addValue(value);
    }

    // setTotal method
    public void setTotal(long total) {
        this.total = total;
    }

    // dump method
    public void dump() throws IOException {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.put("name", name);
        objectNode.put("samples", descriptiveStatistics.getN());
        objectNode.put("total", total);
        objectNode.put("mean", descriptiveStatistics.getMean());
        // add percentiles
        objectNode.put("5", descriptiveStatistics.getPercentile(5));
        objectNode.put("25", descriptiveStatistics.getPercentile(25));
        objectNode.put("50", descriptiveStatistics.getPercentile(50));
        objectNode.put("75", descriptiveStatistics.getPercentile(75));
        objectNode.put("95", descriptiveStatistics.getPercentile(95));
        // write the JSON object to file
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter objectWriter = objectMapper.writer(new DefaultPrettyPrinter());
        objectWriter.writeValue(new File(fileName), objectNode);
    }
}
