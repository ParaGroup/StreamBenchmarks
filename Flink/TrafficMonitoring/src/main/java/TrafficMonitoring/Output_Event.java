package TrafficMonitoring;

public class Output_Event {
	public int roadID;
	public int speed;
	public long ts;

	public Output_Event() {
		roadID = 0;
		speed = 0;
		ts = 0L;
	}

	public Output_Event(int _roadID, int _speed, long _ts) {
		roadID = _roadID;
		speed = _speed;
		ts = _ts;
	}
}
