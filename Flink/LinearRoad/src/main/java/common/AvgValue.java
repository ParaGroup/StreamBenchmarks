package common;

/**
 * {@link AvgValue} is an class that helps to compute an average.
 *
 * @author mjsax
 */
public final class AvgValue {
    /**
     * The current sum over all values.
     */
    private int sum;

    /**
     * The current number of comm values.
     */
    private int count;


    /**
     * Instantiates a new {@link AvgValue} object with initial value.
     *
     * @param initalValue the first value of the average
     */
    public AvgValue(int initalValue) {
        this.sum = initalValue;
        this.count = 1;
    }


    /**
     * Adds a new value to the average.
     *
     * @param value the value to be added to the average
     */
    public void updateAverage(int value) {
        this.sum += value;
        ++this.count;
    }

    /**
     * Returns the current average.
     *
     * @return the current average
     */
    public Integer getAverage() {
        return this.sum / this.count;
    }

}
