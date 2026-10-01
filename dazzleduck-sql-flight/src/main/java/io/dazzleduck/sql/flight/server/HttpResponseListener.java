package io.dazzleduck.sql.flight.server;

import java.io.IOException;

/**
 * An HTTP response body fed by a result stream (Arrow IPC, JSON/JSONL, TSV).
 *
 * <p>For these listeners {@code isCancelled()} means "the response is over": the client went away,
 * a write failed, or the response completed. The stream loop stops on it either way, since nothing
 * more can be written. {@link #writeFailure()} says why a write failed, so a genuine server-side
 * failure is still counted as a stream error rather than treated as a disconnect.
 */
public interface HttpResponseListener {

    /** The exception that ended the response while starting it or writing a batch, or null. */
    Throwable writeFailure();

    /**
     * Whether {@code failure} means the client went away (broken pipe, connection reset: an
     * IOException from the response stream, possibly wrapped, e.g. in an UncheckedIOException), as
     * opposed to the server failing to produce output (e.g. a JSON serialization error, which
     * Jackson also reports as an IOException subclass).
     *
     * <p>The HTTP server must hand these listeners a response stream that reports a gone client as
     * an IOException (the http module's {@code ResponseBodies} does).
     */
    static boolean isClientGone(Throwable failure) {
        // Walks the cause chain. A Jackson exception anywhere in it is a serialization failure.
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof com.fasterxml.jackson.core.JacksonException) {
                return false;
            }
            if (t instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    /** Logs a failed write: at debug when the client simply went away, at error otherwise. */
    static void logWriteFailure(org.slf4j.Logger logger, String where, Throwable failure) {
        if (isClientGone(failure)) {
            logger.atDebug().setCause(failure).log("Client went away during {}", where);
        } else {
            logger.atError().setCause(failure).log("Error in {}", where);
        }
    }
}
