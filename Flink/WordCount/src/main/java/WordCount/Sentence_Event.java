package WordCount;

public class Sentence_Event {
	public String sentence;
	public long ts;

	public Sentence_Event() {
		sentence = "";
		ts = 0L;
	}

	public Sentence_Event(String _sentence, long _ts) {
		sentence = _sentence;
		ts = _ts;
	}
}
