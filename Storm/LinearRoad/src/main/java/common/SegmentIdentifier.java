package common;

import java.io.Serializable;

public class SegmentIdentifier implements Serializable, Cloneable {
    public Integer xway;
    public Short segment;
    public Short direction;

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
