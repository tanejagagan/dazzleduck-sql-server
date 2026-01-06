package io.dazzleduck.sql.scrapper.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.LoggerFactory;

/**
 * Programmatic Logback configuration from HOCON .conf files.
 * This replaces logback.xml to maintain a single configuration format.
 *
 * Configuration example in application.conf:
 * <pre>
 * logging {
 *     level = "INFO"
 *     pattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
 *     loggers {
 *         "io.dazzleduck.sql.scrapper" = "INFO"
 *         "jdk.httpclient" = "WARN"
 *         "org.apache.arrow" = "WARN"
 *     }
 * }
 * </pre>
 */
public class LogbackConfigurator {

    private static final String DEFAULT_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n";
    private static final String DEFAULT_LEVEL = "INFO";

    private static volatile boolean configured = false;

    /**
     * Configure Logback from application.conf.
     * Safe to call multiple times - only configures once.
     */
    public static synchronized void configure() {
        if (configured) {
            return;
        }
        configure(ConfigFactory.load());
        configured = true;
    }

    /**
     * Configure Logback from the given Config object.
     *
     * @param config the HOCON config
     */
    public static void configure(Config config) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        // Get logging configuration with defaults
        String pattern = getStringOrDefault(config, "logging.pattern", DEFAULT_PATTERN);
        String rootLevel = getStringOrDefault(config, "logging.level", DEFAULT_LEVEL);

        // Create console appender
        ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
        consoleAppender.setContext(context);
        consoleAppender.setName("CONSOLE");

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern(pattern);
        encoder.start();

        consoleAppender.setEncoder(encoder);
        consoleAppender.start();

        // Configure root logger
        Logger rootLogger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.toLevel(rootLevel));
        rootLogger.addAppender(consoleAppender);

        // Configure specific loggers from config
        if (config.hasPath("logging.loggers")) {
            Config loggersConfig = config.getConfig("logging.loggers");
            for (String loggerName : loggersConfig.root().keySet()) {
                String level = loggersConfig.getString(loggerName);
                Logger logger = context.getLogger(loggerName);
                logger.setLevel(Level.toLevel(level));
            }
        }

        // Default quiet loggers (reduce noise)
        setLoggerLevel(context, "jdk.httpclient", "WARN");
        setLoggerLevel(context, "org.apache.arrow", "WARN");
    }

    private static void setLoggerLevel(LoggerContext context, String name, String level) {
        Logger logger = context.getLogger(name);
        if (logger.getLevel() == null) {
            logger.setLevel(Level.toLevel(level));
        }
    }

    private static String getStringOrDefault(Config config, String path, String defaultValue) {
        try {
            if (config.hasPath(path)) {
                return config.getString(path);
            }
        } catch (Exception ignored) {
        }
        return defaultValue;
    }

    /**
     * Reset configuration state. For testing only.
     */
    static void reset() {
        configured = false;
    }
}
