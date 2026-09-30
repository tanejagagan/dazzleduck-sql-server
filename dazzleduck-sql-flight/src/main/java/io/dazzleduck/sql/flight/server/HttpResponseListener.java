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
     * IOException from the response stream), as opposed to the server failing to produce output
     * (e.g. a JSON serialization error, which Jackson also reports as an IOException subclass).
     */
    static boolean isClientGone(Throwable failure) {
        return failure instanceof IOException
                && !(failure instanceof com.fasterxml.jackson.core.JacksonException);
    }
}
