package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;

/**
 * Limits on open server-side cursors (Flight SQL statement contexts).
 *
 * A cursor is created each time a client opens a streaming query and lives
 * until the stream is fully consumed or evicted. Without limits, a client can
 * open arbitrarily many cursors to exhaust server connections and memory.
 *
 * <p>{@code cursorTtlMs} evicts a cursor that long after it was created. An idle
 * cursor is closed at once; one whose query is still executing or streaming is
 * closed when that stream ends, so a query may run longer than the TTL.
 */
public record CursorConfig(
        long cursorTtlMs,
        int maxCursorsPerIdentity,
        int maxCursorsTotal
) {

    public static final CursorConfig DEFAULT = new CursorConfig(60_000, 50, 2_000);

    public static CursorConfig fromConfig(Config config) {
        return new CursorConfig(
                config.getLong(ConfigConstants.CURSOR_TTL_MS_KEY),
                config.getInt(ConfigConstants.MAX_CURSORS_PER_IDENTITY_KEY),
                config.getInt(ConfigConstants.MAX_CURSORS_TOTAL_KEY)
        );
    }
}
