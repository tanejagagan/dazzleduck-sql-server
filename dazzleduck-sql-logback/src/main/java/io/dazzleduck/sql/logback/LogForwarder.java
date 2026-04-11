package io.dazzleduck.sql.logback;

import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.common.types.JavaRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Forwards log entries to a remote server via HTTP using Arrow format.
 * Log entries are added directly to the HttpArrowProducer which handles
 * batching and sending.
 */
public final class LogForwarder implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(LogForwarder.class);

    private final LogToArrowConverter converter;
    private final HttpArrowProducer httpProducer;
    private final Map<String, String> resourceMdc;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a LogForwarder with the given configuration.
     *
     * @param config Configuration for the forwarder
     */
    public LogForwarder(LogForwarderConfig config) {
        this.converter = new LogToArrowConverter();
        this.resourceMdc = config.resourceMdc();

        // Create HttpArrowProducer — use static JWT constructor if a token is preconfigured
        this.httpProducer = config.jwt() != null
                ? new HttpArrowProducer(
                        converter.getSchema(),
                        config.baseUrl(),
                        config.jwt(),
                        config.ingestionQueue(),
                        config.httpClientTimeout(),
                        config.minBatchSize(),
                        config.maxBatchSize(),
                        config.maxSendInterval(),
                        config.retryCount(),
                        config.retryIntervalMillis(),
                        config.partitionBy(),
                        config.maxInMemorySize(),
                        config.maxOnDiskSize())
                : new HttpArrowProducer(
                        converter.getSchema(),
                        config.baseUrl(),
                        config.username(),
                        config.password(),
                        config.claims(),
                        config.ingestionQueue(),
                        config.httpClientTimeout(),
                        config.minBatchSize(),
                        config.maxBatchSize(),
                        config.maxSendInterval(),
                        config.retryCount(),
                        config.retryIntervalMillis(),
                        config.partitionBy(),
                        config.maxInMemorySize(),
                        config.maxOnDiskSize(),
                        Clock.systemUTC());

        logger.info("LogForwarder started with baseUrl={}, ingestionQueue={}",
                config.baseUrl(), config.ingestionQueue());
    }

    /**
     * Add a log entry to be forwarded.
     * The entry is converted to Arrow format and added to the producer's queue.
     *
     * @param entry The log entry to forward
     * @return true if the entry was accepted, false if dropped (queue full or not running)
     */
    public boolean addLogEntry(LogEntry entry) {
        if (!running.get() || closed.get()) {
            return false;
        }

        try {
            JavaRow row = convertToJavaRow(entry);
            httpProducer.addRow(row);
            return true;
        } catch (IllegalStateException e) {
            // Queue is full
            logger.debug("Queue full, dropping log entry: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.debug("Failed to add log entry: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Convert a LogEntry to a JavaRow for the HttpArrowProducer.
     * Field order must match the schema from LogToArrowConverter:
     * sequence_number, timestamp (epoch millis), level, logger, thread, message, mdc,
     * throwable, marker, key_value_pairs, caller_class, caller_method, caller_file, caller_line
     */
    private JavaRow convertToJavaRow(LogEntry entry) {
        Object[] fields = new Object[14];
        fields[0] = entry.sequenceNumber();
        fields[1] = entry.timestamp() != null ? entry.timestamp().toEpochMilli() : null;
        fields[2] = entry.level();
        fields[3] = entry.logger();
        fields[4] = entry.thread();
        fields[5] = entry.message();

        // Merge event MDC with resource MDC from config
        Map<String, String> mergedMdc = mergeMdc(entry.mdc(), resourceMdc);
        fields[6] = mergedMdc.isEmpty() ? null : mergedMdc;

        fields[7] = entry.throwable();
        fields[8] = entry.markers().isEmpty() ? null : entry.markers();
        fields[9] = entry.keyValuePairs().isEmpty() ? null : entry.keyValuePairs();
        LogEntry.CallerData cd = entry.callerData();
        fields[10] = cd != null ? cd.className() : null;
        fields[11] = cd != null ? cd.method() : null;
        fields[12] = cd != null ? cd.file() : null;
        fields[13] = cd != null ? cd.line() : null;
        return new JavaRow(fields);
    }

    /**
     * Merge event MDC with resource MDC from config.
     * Resource MDC values are applied as defaults but can be overridden by event MDC.
     *
     * @param eventMdc  MDC from the logging event
     * @param resourceMdc MDC from config
     * @return merged MDC map
     */
    private Map<String, String> mergeMdc(Map<String, String> eventMdc, Map<String, String> resourceMdc) {
        if (resourceMdc.isEmpty()) {
            return eventMdc;
        }
        if (eventMdc.isEmpty()) {
            return resourceMdc;
        }

        // Merge both maps with event MDC taking precedence
        Map<String, String> merged = new HashMap<>(resourceMdc);
        merged.putAll(eventMdc);
        return merged;
    }

    /**
     * Stop the log forwarder gracefully.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping LogForwarder...");
            logger.info("LogForwarder stopped");
        }
    }

    /**
     * Check if the forwarder is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            stop();

            try {
                httpProducer.close();
            } catch (Exception e) {
                logger.error("Error closing HttpArrowProducer", e);
            }

            logger.info("LogForwarder closed");
        }
    }
}
