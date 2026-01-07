package io.dazzleduck.sql.scrapper;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a single collected metric entry from a Prometheus endpoint.
 */
public record CollectedMetric(
    long sNo,
    Instant timestamp,
    String name,
    String type,
    String sourceUrl,
    String collectorId,
    String collectorName,
    String collectorHost,
    Map<String, String> labels,
    double value
) {
    private static final AtomicLong sequenceCounter = new AtomicLong(0);

    /**
     * Create a metric with current timestamp and auto-generated sequence number.
     */
    public CollectedMetric(
            String name,
            String type,
            String sourceUrl,
            String collectorId,
            String collectorName,
            String collectorHost,
            Map<String, String> labels,
            double value) {
        this(sequenceCounter.incrementAndGet(), Instant.now(), name, type, sourceUrl,
             collectorId, collectorName, collectorHost, labels, value);
    }

    /**
     * Reset sequence counter. For testing only.
     */
    static void resetSequence() {
        sequenceCounter.set(0);
    }
}
