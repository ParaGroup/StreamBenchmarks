package applications.bolts.ysb;

// MBCounter class
public class WinCounter {
    public static int counter = 0;
    public static long totalWindow = 0;
    public static long totalWinSize = 0;

    public static synchronized void add(long _totalWindow, long _totalWinSize) {
        counter++;
        totalWindow += _totalWindow;
        totalWinSize += _totalWinSize;
    }

    public static synchronized int getCounter() {
        return counter;
    }

    public static synchronized long getNumWindows() {
        return totalWindow;
    }

    public static synchronized long getTotalWinSize() {
        return totalWinSize;
    }
}
