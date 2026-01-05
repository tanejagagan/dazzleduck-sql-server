package io.dazzleduck.sql.logger.tailing.e2e;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates realistic log entries using Logback for testing
 */
public class LogFileGenerator {

    private static final Logger logger = LoggerFactory.getLogger(LogFileGenerator.class);

    private static final String[] LOG_LEVELS = {"INFO", "DEBUG", "WARN", "ERROR"};
    private static final String[] LOGGER_NAMES = {
            "com.example.ServiceA",
            "com.example.ServiceB",
            "com.example.DatabasePool",
            "com.example.CacheManager",
            "com.example.ApiController"
    };
    private static final String[] MESSAGES = {
            "Processing request",
            "Database query executed successfully",
            "Cache hit for key",
            "HTTP request received",
            "Transaction completed",
            "Connection pool status check",
            "User authentication successful",
            "Data validation passed",
            "Background job started",
            "Metrics collected"
    };

    private final Path logDirectory;
    private final long maxFileSize;
    private final Random random;
    private final AtomicInteger fileCounter;
    private final AtomicInteger lineCounter;
    private org.slf4j.Logger[] loggers;

    public LogFileGenerator(Path logDirectory, long maxFileSize) {
        this.logDirectory = logDirectory;
        this.maxFileSize = maxFileSize;
        this.random = new Random();
        this.fileCounter = new AtomicInteger(1);
        this.lineCounter = new AtomicInteger(0);

        // Pre-create loggers for each logger name
        this.loggers = new org.slf4j.Logger[LOGGER_NAMES.length];
        for (int i = 0; i < LOGGER_NAMES.length; i++) {
            this.loggers[i] = LoggerFactory.getLogger(LOGGER_NAMES[i]);
        }
    }

    /**
     * Generate log entries continuously until stopped
     */
    public void generateLogs(int entriesPerBatch, long intervalMillis) throws IOException, InterruptedException {
        logger.info("Starting log generation: {} entries every {}ms", entriesPerBatch, intervalMillis);

        // Create log directory if it doesn't exist
        Files.createDirectories(logDirectory);

        Path currentFile = createNewLogFile();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                for (int i = 0; i < entriesPerBatch; i++) {
                    writeLogEntry();
                    lineCounter.incrementAndGet();
                }

                // Check if file size exceeded - check the actual file being written to
                if (Files.exists(currentFile) && Files.size(currentFile) >= maxFileSize) {
                    logger.info("File size limit reached for {}, creating new file", currentFile.getFileName());
                    currentFile = createNewLogFile();
                }

                Thread.sleep(intervalMillis);
            }
        } finally {
            logger.info("Log generation stopped. Total lines generated: {}", lineCounter.get());
        }
    }

    private Path createNewLogFile() throws IOException {
        String fileName = String.format("app-%03d.log", fileCounter.getAndIncrement());
        Path filePath = logDirectory.resolve(fileName);

        // Update the system property to point Logback to the new file
        System.setProperty("LOG_FILE", filePath.toAbsolutePath().toString());

        // Force Logback to reload configuration to pick up the new LOG_FILE
        reloadLogbackConfiguration();

        logger.info("Created new log file: {}", fileName);
        return filePath;
    }

    /**
     * Reload Logback configuration to pick up the new LOG_FILE system property
     */
    private void reloadLogbackConfiguration() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(loggerContext);

        try {
            // Reload the logback configuration file
            java.net.URL configUrl = getClass().getClassLoader().getResource("logback-test.xml");
            if (configUrl == null) {
                configUrl = getClass().getClassLoader().getResource("logback.xml");
            }

            if (configUrl != null) {
                configurator.doConfigure(configUrl);
            } else {
                logger.warn("Could not find logback configuration file");
            }
        } catch (JoranException e) {
            logger.error("Error reloading logback configuration", e);
        }

        // Re-initialize loggers after configuration reload
        for (int i = 0; i < LOGGER_NAMES.length; i++) {
            this.loggers[i] = LoggerFactory.getLogger(LOGGER_NAMES[i]);
        }
    }

    private void writeLogEntry() {
        String level = LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
        org.slf4j.Logger selectedLogger = loggers[random.nextInt(loggers.length)];
        String message = MESSAGES[random.nextInt(MESSAGES.length)] + " [id=" + random.nextInt(1000) + "]";

        // Write log using actual Logback logger based on level
        switch (level) {
            case "DEBUG":
                selectedLogger.debug(message);
                break;
            case "INFO":
                selectedLogger.info(message);
                break;
            case "WARN":
                selectedLogger.warn(message);
                break;
            case "ERROR":
                selectedLogger.error(message);
                break;
        }
    }

    public int getTotalLinesGenerated() {
        return lineCounter.get();
    }

    public int getCurrentFileNumber() {
        return fileCounter.get() - 1;
    }
}