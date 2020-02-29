package Constants;

// class YSBConstants
public interface YSBConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/YSB/YSB.properties";
    String DEFAULT_TOPO_NAME = "YSB";

    interface Conf {
        String RUNTIME = "ysb.runtime_sec";
        String NUM_KEYS = "ysb.numKeys";
    }

    interface Component extends BaseComponent {
        String FILTER = "filter";
        String JOINER = "joiner";
        String WINAGG = "winAggregate";
    }

    interface Field extends BaseField {
        String UUID = "uuid";
        String UUID2 = "uuid2";
        String AD_ID = "ad_id";
        String AD_TYPE = "ad_type";
        String EVENT_TYPE = "event_type";
        String TIMESTAMP = "timestamp";
        String IP = "ip";
        String CMP_ID = "cmp_id";
        String COUNTER = "counter";
    }
}
