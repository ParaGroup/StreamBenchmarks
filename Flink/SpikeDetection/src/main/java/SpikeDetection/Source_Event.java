package SpikeDetection;

public class Source_Event {
	public String deviceID;
	public double value;
	public long ts;

	public Source_Event() {
		deviceID = "";
		value = 0d;
		ts = 0L;
	}

	public Source_Event(String _deviceID, double _value, long _ts) {
		deviceID = _deviceID;
		value = _value;
		ts = _ts;
	}
}
