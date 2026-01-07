package io.dazzleduck.sql.scrapper;

import io.dazzleduck.sql.scrapper.config.LogbackConfigurator;

/**
 * Main entry point for the DazzleDuck Metrics Scrapper.
 *
 * This standalone application:
 * 1. Scrapes Prometheus metrics from configured endpoints
 * 2. Buffers metrics with tailing mode
 * 3. Forwards to a remote server in Apache Arrow format
 *
 * Usage:
 * <pre>
 * java -jar dazzleduck-sql-scrapper.jar
 * java -jar dazzleduck-sql-scrapper.jar --config /path/to/config.conf
 * java -jar dazzleduck-sql-scrapper.jar -c /path/to/config.conf
 * </pre>
 *
 * Configuration uses HOCON format (.conf files). See application.conf for defaults.
 */
public class Main {

    /**
     * Application entry point. Delegates to MetricsCollectorServer.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        // Configure logging from .conf before anything else
        LogbackConfigurator.configure();
        MetricsCollectorServer.main(args);
    }
}
