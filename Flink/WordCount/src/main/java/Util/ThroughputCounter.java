package Util;

// ThroughputCounter class
public class ThroughputCounter {
    public static long counter = 0;
    public static long bytes = 0;

    public static synchronized void add(long _val, long _bytes) {
        counter += _val;
        bytes += _bytes;
    }

    public static synchronized long getValue() {
        return counter;
    }

    public static synchronized long getBytes() {
        return bytes;
    }
}
