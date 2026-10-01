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
     * Whether {@code failure} means the client went away, as opposed to the server failing to produce
     * output (e.g. a JSON serialization error, which Jackson also reports as an IOException subclass).
     * How Helidon reports a gone client depends on the protocol:
     * <ul>
     *   <li>HTTP/1.1: an IOException (broken pipe, connection reset), wrapped in an UncheckedIOException;</li>
     *   <li>HTTP/2, connection closed: a {@code CloseConnectionException}
     *       ({@code ServerConnectionException} extends it);</li>
     *   <li>HTTP/2, stream reset with the connection still open: the write waits for flow control the
     *       client will never grant, then fails with an {@code Http2Exception}; or, if the reset
     *       arrived first, an IllegalStateException from Helidon's HTTP/2 stream ("Stream is already
     *       closed.").</li>
     * </ul>
     * Helidon's types are matched by name: this module doesn't depend on Helidon.
     */
    static boolean isClientGone(Throwable failure) {
        // Walks the cause chain. A Jackson exception anywhere in it is a serialization failure.
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof com.fasterxml.jackson.core.JacksonException) {
                return false;
            }
            if (t instanceof IOException || isHelidonDisconnect(t)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isHelidonDisconnect(Throwable t) {
        for (Class<?> type = t.getClass(); type != null; type = type.getSuperclass()) {
            switch (type.getName()) {
                case "io.helidon.webserver.CloseConnectionException", "io.helidon.http.http2.Http2Exception" -> {
                    return true;
                }
                default -> { }
            }
        }
        StackTraceElement[] thrownFrom = t.getStackTrace();
        return t instanceof IllegalStateException
                && thrownFrom.length > 0
                && thrownFrom[0].getClassName().startsWith("io.helidon.webserver.http2.Http2ServerStream");
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
