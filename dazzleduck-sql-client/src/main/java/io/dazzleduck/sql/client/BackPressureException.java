package io.dazzleduck.sql.client;

/**
 * Exception thrown when the server signals back pressure due to resource exhaustion.
 * This is typically returned as HTTP 429 (Too Many Requests) or gRPC RESOURCE_EXHAUSTED.
 * <p>
 * Back pressure is a temporary condition indicating the server cannot accept more data
 * at the current rate. Clients should implement exponential backoff when receiving this exception.
 */
public class BackPressureException extends RuntimeException {

    private static final long DEFAULT_WAIT_MILLIS = 5000L;

    private final long suggestedWaitMillis;

    public BackPressureException(String message) {
        this(message, DEFAULT_WAIT_MILLIS);
    }

    public BackPressureException(String message, long suggestedWaitMillis) {
        super(message);
        this.suggestedWaitMillis = suggestedWaitMillis;
    }

    public BackPressureException(String message, Throwable cause) {
        this(message, cause, DEFAULT_WAIT_MILLIS);
    }

    public BackPressureException(String message, Throwable cause, long suggestedWaitMillis) {
        super(message, cause);
        this.suggestedWaitMillis = suggestedWaitMillis;
    }

    /**
     * Returns the suggested wait time in milliseconds before retrying.
     * This may come from the server's Retry-After header or a default value.
     */
    public long getSuggestedWaitMillis() {
        return suggestedWaitMillis;
    }
}
