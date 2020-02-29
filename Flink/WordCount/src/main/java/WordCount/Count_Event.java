package WordCount;

public class Count_Event {
	public String word;
	public long count;
	public long ts;

	public Count_Event() {
		word = "";
		count = 0L;
		ts = 0L;
	}

	public Count_Event(String _word, long _count, long _ts) {
		word = _word;
		count = _count;
		ts = _ts;
	}
}
