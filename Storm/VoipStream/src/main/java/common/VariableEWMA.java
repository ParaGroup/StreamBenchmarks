package common;

/**
 * VariableEWMA represents the exponentially weighted moving average of a series of
 * numbers. Unlike SimpleEWMA, it supports a custom age, and thus uses more memory.
 * <p/>
 * Original code: https://github.com/VividCortex/ewma
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VariableEWMA {
    /**
     * For best results, the moving average should not be initialized to the
     * samples it sees immediately. The book "Production and Operations
     * Analysis" by Steven Nahmias suggests initializing the moving average to
     * the mean of the first 10 samples. Until the VariableEwma has seen this
     * many samples, it is not "ready" to be queried for the value of the
     * moving average. This adds some memory cost.
     */
    protected static final int WARMUP_SAMPLES = 10;

    /**
     * The current value of the average. After adding with add(), this is
     * updated to reflect the average of all values seen thus far.
     */
    protected double average;

    /**
     * The multiplier factor by which the previous samples decay.
     */
    protected double decay;

    /**
     * The number of samples added to this instance.
     */
    protected int count;

    /**
     * @param age The age is related to the decay factor alpha by the formula
     *            given for the DECAY constant. It signifies the average age of the samples
     *            as time goes to infinity.
     */
    public VariableEWMA(double age) {
        decay = 2 / (age + 1);
    }

    /**
     * Add adds a value to the series and updates the moving average.
     *
     * @param value The value to be added
     */
    public void add(double value) {
        if (average < WARMUP_SAMPLES) {
            count++;
            average += value;
        } else if (average == WARMUP_SAMPLES) {
            average = average / WARMUP_SAMPLES;
            count++;
        } else {
            average = (value * decay) + (average * (1 - decay));
        }
    }

    /**
     * @return The current value of the average, or 0.0 if the series hasn't warmed up yet.
     */
    public double getAverage() {
        if (average <= WARMUP_SAMPLES) {
            return 0.0;
        }

        return average;
    }
}
