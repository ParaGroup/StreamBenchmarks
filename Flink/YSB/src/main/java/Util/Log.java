package Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

// Log class
public class Log {
    static {
        Level level;
        // restrict messages from this package
        level = Level.toLevel(System.getenv("LOG_LEVEL"), Level.INFO);
        Configurator.setLevel("YSB", level);
        // restrict messages from other components
        level = Level.toLevel(System.getenv("ROOT_LOG_LEVEL"), Level.ERROR);
        Configurator.setRootLevel(level);
    }

    // get method
    public static Logger get(Class klass) {
        return LoggerFactory.getLogger(klass);
    }
}
