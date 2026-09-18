package io.dazzleduck.sql.commons.ingestion;

/**
 * Thrown when a {@link PartitionedIngestionQueue} cannot evaluate its {@code partition_expression}
 * over a batch (e.g. a transient read/connection failure, or the expression not resolving against
 * the batch's schema). This is a <b>server-side</b> failure, not a client bad-request: it maps to a
 * retryable status (HTTP 503, gRPC/Flight {@code UNAVAILABLE}) so a producer resends the batch
 * rather than treating it as permanently rejected.
 *
 * <p>Contrast with a batch whose rows genuinely span more than one partition, which is a permanent
 * caller error surfaced as {@link IllegalArgumentException} → {@code INVALID_ARGUMENT} / HTTP 400.
 */
public class PartitionEvaluationException extends RuntimeException {
    public PartitionEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
