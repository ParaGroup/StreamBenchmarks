package common;

public class CountTuple {
    public Short minuteNumber;
    public Integer xway;
    public Short segment;
    public Short direction;
    public Integer count;

    public boolean isProgressTuple() {
        return xway == null;
    }
}
