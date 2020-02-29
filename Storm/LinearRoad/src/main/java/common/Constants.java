package common;

/**
 * Constants related to the benchmark constraints. (See page 483 "Linear Road: A Streaming Data Management Benchmark")
 *
 * @author richter
 * @author mjsax
 */
public final class Constants {

    /**
     * 0...10799
     */
    public final static short MAX_TIME_SECONDS = 10799;
    public final static short NUMBER_OF_SECONDS = MAX_TIME_SECONDS + 1;

    /**
     * 0...MAXINT
     */
    public final static int MAX_VID = Integer.MAX_VALUE;
    public final static long NUMBER_OF_VIDS = MAX_VID + 1L;

    /**
     * 0...100
     */
    public static final int MAX_SPEED = 100;
    public static final int NUMBER_OF_SPEEDS = MAX_SPEED + 1;

    /* lanes: 0..4 */
    public static final Short l0 = (short) 0;
    public static final Short l1 = (short) 1;
    public static final Short l2 = (short) 2;
    public static final Short l3 = (short) 3;
    public static final Short l4 = (short) 4;
    public static final short ENTRANCE_LANE = l0;
    public static final short EXIT_LANE = l4;


    public static final Short EASTBOUND = (short) 0;
    public static final Short WESTBOUND = (short) 1;

    /**
     * 0...100
     */
    public static final short MAX_SEGMENT = 100;
    public static final short NUMBER_OF_SEGMENT = MAX_SEGMENT + 1;

    /**
     * 0...527999
     */
    public final static int MAX_POSITION = 527999;
    public final static int NUMBER_OF_POSITIONS = MAX_POSITION + 1;

    /**
     * "If the LAV [...] is greater than or equal to 40 MPH, [...], no toll is assessed."
     */
    public static final int TOLL_LAV_THRESHOLD = 40;
    /**
     * "[...], or if the number of vehicles [...] was 50 or less [...], no toll is assessed."
     */
    public static final int TOLL_NUM_CARS_THRESHOLD = 50;

    /**
     * TODO: search in benchmark specification
     */
    public final static int INITIAL_TOLL = 20;


    private Constants() {
    }
}
