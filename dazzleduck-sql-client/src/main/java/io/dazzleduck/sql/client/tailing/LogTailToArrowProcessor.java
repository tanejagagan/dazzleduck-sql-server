package io.dazzleduck.sql.client.tailing;

import io.dazzleduck.sql.client.HttpSender;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Main processor that continuously:
 * 1. Reads new log lines from file (tracking byte position)
 * 2. Converts JSON to Apache Arrow
 * 3. Sends to server via HttpSender
 */
public final class LogTailToArrowProcessor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LogTailToArrowProcessor.class);
    private static final long DEFAULT_POLL_INTERVAL_MS = 30_000; // 30 seconds

    private final LogFileTailReader tailReader;
    private final JsonToArrowConverter arrowConverter;
    private final HttpSender httpSender;
    private final ScheduledExecutorService scheduler;
    private final long pollIntervalMs;

    private volatile boolean running = false;

    public LogTailToArrowProcessor(
            String logFilePath,
            HttpSender httpSender,
            long pollIntervalMs
    ) {
        Objects.requireNonNull(logFilePath, "logFilePath must not be null");
        Objects.requireNonNull(httpSender, "httpSender must not be null");

        if (pollIntervalMs <= 0) {
            throw new IllegalArgumentException("pollIntervalMs must be positive");
        }

        this.tailReader = new LogFileTailReader(logFilePath);
        this.arrowConverter = new JsonToArrowConverter();
        this.httpSender = httpSender;
        this.pollIntervalMs = pollIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-tail-processor");
            t.setDaemon(true);
            return t;
        });

        logger.info("Initialized LogTailToArrowProcessor: file={}, pollInterval={}ms",
                logFilePath, pollIntervalMs);
    }

    public LogTailToArrowProcessor(String logFilePath, HttpSender httpSender) {
        this(logFilePath, httpSender, DEFAULT_POLL_INTERVAL_MS);
    }

    /**
     * Start continuous processing
     */
    public void start() {
        if (running) {
            logger.warn("Processor already running");
            return;
        }

        running = true;
        logger.info("Starting log tail processor with {}ms poll interval", pollIntervalMs);

        scheduler.scheduleAtFixedRate(
                this::processNewLogs,
                0,
                pollIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stop processing
     */
    public void stop() {
        if (!running) {
            return;
        }

        logger.info("Stopping log tail processor");
        running = false;

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate gracefully, forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Processor stopped");
    }

    private void processNewLogs() {
        try {
            // Step 1: Read new lines from log file
            List<String> newLines = tailReader.readNewLines();

            if (newLines.isEmpty()) {
                return;
            }

            logger.info("Processing {} new log entries (file position: {} bytes)",
                    newLines.size(), tailReader.getLastReadPosition());

            // Step 2: Convert JSON to Arrow
            VectorSchemaRoot arrowBatch = arrowConverter.convertToArrow(newLines);

            if (arrowBatch == null) {
                logger.error("Failed to convert logs to Arrow format");
                return;
            }

            try {
                // Step 3: Send to server using existing HttpSender
                byte[] arrowBytes = toArrowBytes(arrowBatch);
                httpSender.enqueue(arrowBytes);

                logger.debug(
                        "Successfully queued {} records ({} bytes) for sending",
                        arrowBatch.getRowCount(),
                        arrowBytes.length
                );
                logger.debug("Successfully queued {} records for sending", arrowBatch.getRowCount());
            } finally {
                arrowBatch.close();
            }

        } catch (Exception e) {
            logger.error("Error processing logs", e);
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void close() {
        stop();
        arrowConverter.close();
        httpSender.close();
    }

    private static byte[] toArrowBytes(VectorSchemaRoot root) {
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)
        ) {
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Arrow batch", e);
        }
    }

}