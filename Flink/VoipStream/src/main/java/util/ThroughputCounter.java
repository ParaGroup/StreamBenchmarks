package util;

// ThroughputCounter class
public class ThroughputCounter {
    public static long counter = 0;

    public static synchronized void add(long _val) {
        counter += _val;
    }

    public static synchronized long getValue() {
        return counter;
    }
}
