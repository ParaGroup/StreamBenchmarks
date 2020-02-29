package common;

import util.Time;

/**
 * A {@link PositionReport} from the LRB data generator.<br />
 * <br />
 * Position reports do have the following attributes: TYPE=0, TIME, VID, Spd, XWay, Lane, Dir, Seg, Pos
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in LRB seconds)</li>
 * <li>VID: the unique vehicle ID</li>
 * <li>Spd: the speed of the vehicle (0...100)</li>
 * <li>XWay: the ID of the expressway the vehicle is driving on (1...L-1)</li>
 * <li>Lane: the ID of the lane the vehicle is using (0...4)</li>
 * <li>Dir: the direction the vehicle is driving (0 for Eastbound; 1 for Westbound)</li>
 * <li>Seg: the ID of the expressway segment the vehicle in on (0...99)</li>
 * <li>Pos: the position in feet of the vehicle (distance to expressway Westbound point; 0...527999</li>
 * </ul>
 *
 * @author mjsax
 */
public class PositionReport implements Cloneable {
    public Short type;
    public Integer time;
    public Integer vid;
    public Integer speed;
    public Integer xway;
    public Short lane;
    public Short direction;
    public Short segment;
    public Integer position;

    public boolean isOnExitLane() {
        return lane == Constants.EXIT_LANE;
    }

    public short getMinuteNumber() {
        return Time.getMinute(time.shortValue());
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
