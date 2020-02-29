package TrafficMonitoring;

public class Speed_Event {
	public int roadID;
	public int avg_speed;
	public int count;
	public long ts;

	public Speed_Event() {
		roadID = 0;
		avg_speed = 0;
		count = 0;
		ts = 0L;
	}

	public Speed_Event(int _roadID, int _avg_speed, int _count, long _ts) {
		roadID = _roadID;
		avg_speed = _avg_speed;
		count = _count;
		ts = _ts;
	}
}
