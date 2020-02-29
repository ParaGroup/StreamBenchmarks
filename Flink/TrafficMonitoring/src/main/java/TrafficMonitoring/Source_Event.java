package TrafficMonitoring;

public class Source_Event {
	public String vehicleID;
	public double latitude;
	public double longitude;
	public double speed;
	public int bearing;
	public long ts;

	public Source_Event() {
		vehicleID = "";
		latitude = 0d;
		longitude = 0d;
		speed = 0d;
		bearing = 0;
		ts = 0L;
	}

	public Source_Event(String _vehicleID, double _latitude, double _longitude, double _speed, int _bearing, long _ts) {
		vehicleID = _vehicleID;
		latitude = _latitude;
		longitude = _longitude;
		speed = _speed;
		bearing = _bearing;
		ts = _ts;
	}
}
