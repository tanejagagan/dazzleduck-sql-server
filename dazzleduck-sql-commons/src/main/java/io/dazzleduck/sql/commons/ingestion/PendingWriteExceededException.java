package io.dazzleduck.sql.commons.ingestion;

public class PendingWriteExceededException extends Exception {

    private static final int DEFAULT_RETRY_AFTER_SECONDS = 5;

    private final int retryAfterSeconds;

    public PendingWriteExceededException(long currentPending, long maxPending) {
        this(currentPending, maxPending, DEFAULT_RETRY_AFTER_SECONDS);
    }

    public PendingWriteExceededException(long currentPending, long maxPending, int retryAfterSeconds) {
        super("Pending write limit exceeded: current %d bytes, max %d bytes".formatted(currentPending, maxPending));
        this.retryAfterSeconds = retryAfterSeconds;
    }

    /**
     * Returns the suggested wait time in seconds before retrying.
     */
    public int getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
