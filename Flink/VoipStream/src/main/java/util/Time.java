package util;

/**
 * Helper class to compute the current minute of given millisecond.
 *
 * @author msoyka
 * @author mjsax
 */
public class Time {

    private Time() {
    }

    /**
     * Computes the 'minute number' if a time (in seconds).
     * <p/>
     * The 'minute number' m is computed as: {@code m = floor(timestamp / 60) + 1}
     *
     * @param timestamp the timestamp value in seconds
     * @return the 'minute number' if the given timestamp
     */
    public static short getMinute(long timestamp) {
        assert (timestamp >= 0);
        return (short) ((timestamp / 60) + 1);
    }

    public static short getMinute(short timestamp) {
        assert (timestamp >= 0);
        return (short) ((timestamp / 60) + 1);
    }
}
