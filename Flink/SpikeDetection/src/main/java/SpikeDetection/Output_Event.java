package SpikeDetection;

public class Output_Event {
	public String deviceID;
	public double moving_avg;
	public double value;
	public long ts;

	public Output_Event() {
		deviceID = "";
		moving_avg = 0d;
		value = 0d;
		ts = 0L;
	}

	public Output_Event(String _deviceID, double _moving_avg, double _value, long _ts) {
		deviceID = _deviceID;
		moving_avg = _moving_avg;
		value = _value;
		ts = _ts;
	}
}
