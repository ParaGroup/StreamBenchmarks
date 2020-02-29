package Constants;

/** 
 *  @author  Alessandra Fais
 *  @version May 2019
 *  
 *  Constants set for all applications.
 */ 
public interface BaseConstants {
    String HELP = "help";

    interface BaseComponent {
        String SPOUT = "spout";
        String SINK  = "sink";
    }

    interface BaseField {
        String TIMESTAMP = "timestamp";
    }
}
