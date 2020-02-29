package applications.spout;

// MBCounter class
public class MBCounter {
    public static long readBytes = 0;

    public static synchronized void add(long _readBytes) {
        readBytes += _readBytes;
    }

    public static synchronized long getReadBytes() {
        return readBytes;
    }
}
