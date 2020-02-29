package Util;

import java.util.List;
import java.util.ArrayList;

// Sampler class
public class Sampler {
    private final long samplesPerSeconds;
    private List<Double> samples;
    private long epoch;
    private long counter;
    private long total;

    // constructor
    public Sampler() {
        this(0);
    }

    // constructor
    public Sampler(long samplesPerSeconds) {
        this.samplesPerSeconds = samplesPerSeconds;
        epoch = System.nanoTime();
        counter = 0;
        total = 0;
        samples = new ArrayList<>();
    }

    // add method
    public void add(double value) {
        add(value, 0);
    }

    // add method
    public void add(double value, long timestamp) {
        total++;
        // add samples according to the sample rate
        double seconds = (timestamp - epoch) / 1e9;
        if (samplesPerSeconds == 0 || counter <= samplesPerSeconds * seconds) {
            samples.add(value);
            counter++;
        }
    }

    // getValues method
    public List<Double> getValues() {
        return samples;
    }

    // getTotal method
    public long getTotal() {
        return total;
    }
}
