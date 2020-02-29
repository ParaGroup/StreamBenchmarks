package WordCount;

public class Word_Event {
	public String word;
	public long ts;

	public Word_Event() {
		word = "";
		ts = 0L;
	}

	public Word_Event(String _word, long _ts) {
		word = _word;
		ts = _ts;
	}
}
