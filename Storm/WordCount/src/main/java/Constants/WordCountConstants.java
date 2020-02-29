package Constants;

/** 
 *  @author  Alessandra Fais
 *  @version June 2019
 *  
 *  Constants peculiar of the WordCount application.
 */ 
public interface WordCountConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/wordcount/wc.properties";
    String DEFAULT_TOPO_NAME = "WordCount";

    interface Conf {
        String RUNTIME = "wc.runtime_sec";
        String BUFFER_SIZE = "wc.buffer_size";
        String POLLING_TIME = "wc.polling_time_ms";
        String FILE_SOURCE = "file";
        String GEN_SOURCE = "generator";
        String SPOUT_PATH = "wc.spout.path";
    }
    
    interface Component extends BaseComponent {
        String SPLITTER = "splitter";
        String COUNTER = "counter";
    }

    interface Field extends BaseField {
        String LINE = "line";
        String WORD = "word";
        String COUNT = "count";
    }
}
