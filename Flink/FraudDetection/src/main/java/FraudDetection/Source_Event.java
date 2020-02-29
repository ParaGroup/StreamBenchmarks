package FraudDetection;

public class Source_Event {
	public String entityID;
	public String transaction;
	public long ts;

	public Source_Event() {
		entityID = "";
		transaction = "";
		ts = 0L;
	}

	public Source_Event(String _entityID, String _transaction, long _ts) {
		entityID = _entityID;
		transaction = _transaction;
		ts = _ts;
	}
}
