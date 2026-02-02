package io.dazzleduck.sql.logger;

import org.slf4j.ILoggerFactory;
import org.slf4j.IMarkerFactory;
import org.slf4j.helpers.BasicMarkerFactory;
import org.slf4j.spi.MDCAdapter;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * SLF4J 2.0 service provider that forwards logs to a remote server via Arrow format.
 */
public class ArrowSLF4JServiceProvider implements SLF4JServiceProvider {

    public static final String REQUESTED_API_VERSION = "2.0.99";

    private final ArrowSimpleLoggerFactory loggerFactory = new ArrowSimpleLoggerFactory();
    private final ArrowMDCAdapter mdcAdapter = new ArrowMDCAdapter();
    private final IMarkerFactory markerFactory = new BasicMarkerFactory();

    @Override
    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    @Override
    public MDCAdapter getMDCAdapter() {
        return mdcAdapter;
    }

    @Override
    public IMarkerFactory getMarkerFactory() {
        return markerFactory;
    }

    @Override
    public String getRequestedApiVersion() {
        return REQUESTED_API_VERSION;
    }

    @Override
    public void initialize() {
        // Initialize the shared producer
        loggerFactory.initialize();

        // Register shutdown hook to flush logs on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(loggerFactory::shutdown, "arrow-logger-shutdown"));
    }
}
