package common;

public class Constants {
    public final static int VAR_DETECT_APROX_SIZE = 180000;
    public final static double VAR_DETECT_ERROR_RATE = 0.05;

    public final static double ACD_THRESHOLD_MIN = 5.0;
    public final static double ACD_THRESHOLD_MAX = 10.0;
    public final static double ACD_DECAY_FACTOR = 86400.0;
    public final static double ACD_WEIGHT = 3.0;

    public final static double URL_THRESHOLD_MIN = 0.5;
    public final static double URL_THRESHOLD_MAX = 1.0;
    public final static double URL_WEIGHT = 3.0;

    public final static double FOFIR_THRESHOLD_MIN = 2.0;
    public final static double FOFIR_THRESHOLD_MAX = 10.0;
    public final static double FOFIR_WEIGHT = 2.0;

    public final static int CT24_NUM_ELEMENTS = 180000;
    public final static int CT24_BUCKETS_PER_ELEMENT = 10;
    public final static int CT24_BUCKETS_PER_WORD = 16;
    public final static double CT24_BETA = 0.9917;

    public final static int ECR24_NUM_ELEMENTS = 180000;
    public final static int ECR24_BUCKETS_PER_ELEMENT = 10;
    public final static int ECR24_BUCKETS_PER_WORD = 16;
    public final static double ECR24_BETA = 0.9917;

    public final static int ECR_NUM_ELEMENTS = 180000;
    public final static int ECR_BUCKETS_PER_ELEMENT = 10;
    public final static int ECR_BUCKETS_PER_WORD = 16;
    public final static double ECR_BETA = 0.9672;

    public final static int ENCR_NUM_ELEMENTS = 180000;
    public final static int ENCR_BUCKETS_PER_ELEMENT = 10;
    public final static int ENCR_BUCKETS_PER_WORD = 16;
    public final static double ENCR_BETA = 0.9672;

    public final static int RCR_NUM_ELEMENTS = 180000;
    public final static int RCR_BUCKETS_PER_ELEMENT = 10;
    public final static int RCR_BUCKETS_PER_WORD = 16;
    public final static double RCR_BETA = 0.9672;
}
