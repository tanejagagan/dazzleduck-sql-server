package io.dazzleduck.sql.client.tailing;

import io.dazzleduck.sql.client.HttpSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main entry point for log tail to Arrow processor
 *
 * Usage:
 *   java LogProcessorMain <log-file-path> <server-url> <username> <password> <target-path> [poll-interval-ms]
 *
 * Example:
 *   java LogProcessorMain /var/log/app.log http://localhost:8094 admin secret123 /logs/app 30000
 */
public final class LogProcessorMain {

    private static final Logger logger = LoggerFactory.getLogger(LogProcessorMain.class);
    private static final long DEFAULT_POLL_INTERVAL_MS = 30_000; // 30 seconds
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);
    private static final long MIN_BATCH_SIZE = 1;
    private static final Duration MAX_SEND_INTERVAL = Duration.ofSeconds(10);
    private static final long MAX_IN_MEMORY_SIZE = 100 * 1024 * 1024; // 100MB
    private static final long MAX_ON_DISK_SIZE = 1024 * 1024 * 1024; // 1GB

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 5) {
            System.err.println("Usage: java LogProcessorMain <log-file-path> <server-url> <username> <password> <target-path> [poll-interval-ms]");
            System.err.println("Example: java LogProcessorMain /var/log/app.log http://localhost:8080 admin secret123 /logs/app 30000");
            System.exit(1);
        }

        String logFilePath = args[0];
        String serverUrl = args[1];
        String username = args[2];
        String password = args[3];
        String targetPath = args[4];
        long pollIntervalMs = args.length > 5 ? Long.parseLong(args[5]) : DEFAULT_POLL_INTERVAL_MS;

        logger.info("=== DazzleDuck Log Tail to Arrow Processor ===");
        logger.info("Log file: {}", logFilePath);
        logger.info("Server URL: {}", serverUrl);
        logger.info("Username: {}", username);
        logger.info("Target path: {}", targetPath);
        logger.info("Poll interval: {}ms", pollIntervalMs);

        // Create converter to get schema
        JsonToArrowConverter converter = new JsonToArrowConverter();

        // Create HttpSender with existing implementation
        HttpSender httpSender = new HttpSender(
                converter.getSchema(),
                serverUrl,
                username,
                password,
                targetPath,
                HTTP_TIMEOUT,
                MIN_BATCH_SIZE,
                MAX_SEND_INTERVAL,
                MAX_IN_MEMORY_SIZE,
                MAX_ON_DISK_SIZE
        );

        converter.close(); // Close temporary converter

        // Create and start processor
        LogTailToArrowProcessor processor = new LogTailToArrowProcessor(
                logFilePath,
                httpSender,
                pollIntervalMs
        );

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            processor.close();
        }));

        // Start processing
        processor.start();

        logger.info("Processor started. Press Ctrl+C to stop.");

        // Keep main thread alive
        while (processor.isRunning()) {
            Thread.sleep(1000);
        }
    }
}