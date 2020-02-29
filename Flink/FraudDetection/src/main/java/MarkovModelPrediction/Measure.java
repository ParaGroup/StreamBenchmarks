package MarkovModelPrediction;

public class Measure {
	private static long time = 0;

	public static synchronized void set(long _time) {
		time = _time;
	}

	public static synchronized long get() {
		return time;
	}
}