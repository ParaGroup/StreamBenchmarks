package common;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class CallDetailRecord {
    public String callingNumber;
    public String calledNumber;
    public long answerTimestamp;
    public int callDuration;
    public boolean callEstablished;

    // needs to be public to be POJO
    public CallDetailRecord() {
    }

    public CallDetailRecord(String str) {
        String[] psb = str.split(",");

        callingNumber = psb[0];
        calledNumber = psb[1];

        String dateTime = psb[2];
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        answerTimestamp = DateTime.parse(dateTime, formatter).getMillis() / 1000;
        callDuration = Integer.parseInt(psb[3]);
        callEstablished = Boolean.parseBoolean(psb[4]);
    }
}
