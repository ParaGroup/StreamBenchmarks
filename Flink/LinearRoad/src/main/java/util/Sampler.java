package util;

import java.util.ArrayList;
import java.util.List;

public class Sampler {
    private final long samplesPerSeconds;
    private List<Double> samples;
    private long epoch;
    private long counter;
    private long total;

    public Sampler() {
        this(0);
    }

    public Sampler(long samplesPerSeconds) {
        this.samplesPerSeconds = samplesPerSeconds;
        epoch = System.nanoTime();
        counter = 0;
        total = 0;
        samples = new ArrayList<>();
    }

    public void add(double value) {
        add(value, 0);
    }

    public void add(double value, long timestamp) {
        total++;

        // add samples according to the sample rate
        double seconds = (timestamp - epoch) / 1e9;
        if (samplesPerSeconds == 0 || counter <= samplesPerSeconds * seconds) {
            samples.add(value);
            counter++;
        }
    }

    public List<Double> getValues() {
        return samples;
    }

    public long getTotal() {
        return total;
    }
}
