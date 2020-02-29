package util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {
    static {
        Level level;

        // restrict messages from this package
        level = Level.toLevel(System.getenv("LOG_LEVEL"), Level.INFO);
        Configurator.setLevel("LinearRoad", level);

        // restrict messages from other components
        level = Level.toLevel(System.getenv("ROOT_LOG_LEVEL"), Level.ERROR);
        Configurator.setRootLevel(level);
    }

    public static Logger get(Class klass) {
        return LoggerFactory.getLogger(klass);
    }
}
