package FraudDetection;

public class Output_Event {
	public String entityID;
	public double score;
	public long ts;

	public Output_Event() {
		entityID = "";
		score = 0d;
		ts = 0L;
	}

	public Output_Event(String _entityID, double _score, long _ts) {
		entityID = _entityID;
		score = _score;
		ts = _ts;
	}
}
