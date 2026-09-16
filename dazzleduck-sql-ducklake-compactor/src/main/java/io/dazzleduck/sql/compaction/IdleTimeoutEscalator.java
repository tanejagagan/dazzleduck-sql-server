package io.dazzleduck.sql.compaction;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decides whether a compaction/housekeeping failure looks like Postgres killing an idle-in-
 * transaction metadata connection, and if so builds the DETACH/re-ATTACH script that raises that
 * catalog's connection-scoped {@code idle_in_transaction_session_timeout} for next time.
 *
 * <p>Deliberately message-substring based and deliberately conservative: a failure with no
 * Postgres-specific text anywhere in its cause chain (observed in practice as a bare
 * {@code Failed to commit: Failed to execute query "ROLLBACK"}) will NOT be detected. Under-
 * matching is the safer failure mode here — a missed escalation just means today's existing
 * behavior, where over-matching risks escalating on an unrelated commit failure.
 *
 * <p>State is in-memory only, per database, and lost on restart by design: nothing durable
 * changes in Postgres (no {@code ALTER DATABASE}), so a restart naturally resets to
 * "not yet escalated," which is fine.
 */
class IdleTimeoutEscalator {

    private static final List<String> SIGNATURES = List.of(
            "idle-in-transaction",
            "idle_in_transaction",
            "ssl connection has been closed unexpectedly",
            "server closed the connection",
            "terminating connection due to");

    /** Depth-bounded so a pathological self-referential cause cycle cannot loop forever. */
    private static final int MAX_CAUSE_DEPTH = 20;

    private final CompactionConfig config;
    private final ConcurrentHashMap<String, AtomicLong> currentTimeoutMs = new ConcurrentHashMap<>();

    IdleTimeoutEscalator(CompactionConfig config) {
        this.config = config;
    }

    /**
     * Returns the multi-statement script to run (via {@code ConnectionPool.executeOnSingleton})
     * to escalate {@code database}'s idle timeout, or empty if escalation does not apply: the
     * feature is off, the database has no {@code postgres_metadata} entry, the failure doesn't
     * look idle-timeout-shaped, or the database is already at the configured ceiling.
     */
    Optional<String> reattachScriptIfEscalationNeeded(String database, Throwable failure) {
        if (!config.idleInTransactionTimeoutAdaptive()) {
            return Optional.empty();
        }
        PostgresMetadataConfig metadata = config.postgresMetadata().get(database);
        if (metadata == null || !isIdleInTransactionTimeout(failure)) {
            return Optional.empty();
        }

        long maxMs = config.idleInTransactionTimeoutMax().toMillis();
        AtomicLong tracked = currentTimeoutMs.computeIfAbsent(database, k -> new AtomicLong(0));
        long previous = tracked.get();
        if (previous >= maxMs) {
            return Optional.empty();
        }
        long next = previous == 0 ? config.idleInTransactionTimeout().toMillis() : Math.min(previous * 2, maxMs);
        if (!tracked.compareAndSet(previous, next)) {
            // Another thread escalated this database concurrently; let that attempt win.
            return Optional.empty();
        }

        // The "postgres:" sub-scheme is required, not optional: "ducklake:host=..." with no scheme
        // silently falls back to a LOCAL DuckDB file named after the literal connection string
        // (verified empirically — zero ducklake_* tables ever appear in Postgres without it, and a
        // stray file named after the connection string appears in the working directory instead).
        // Without it, this DETACH/re-ATTACH would silently switch to a disconnected, empty catalog.
        String attachSql = "ATTACH 'ducklake:postgres:%s options=-c\\ idle_in_transaction_session_timeout=%dms' AS %s %s"
                .formatted(metadata.connectionString(), next, database, metadata.attachOptions());
        // splitStatements only splits on "; *\n|;$" — the newline before the second statement and
        // the trailing semicolon at end-of-string are both required for ConnectionPool to see two
        // statements rather than one malformed one.
        String script = "DETACH \"%s\";\n%s;".formatted(database, attachSql);
        return Optional.of(script);
    }

    static boolean isIdleInTransactionTimeout(Throwable t) {
        int depth = 0;
        for (Throwable current = t; current != null && depth < MAX_CAUSE_DEPTH; current = current.getCause(), depth++) {
            String message = current.getMessage();
            if (message == null) {
                continue;
            }
            String lower = message.toLowerCase(java.util.Locale.ROOT);
            for (String signature : SIGNATURES) {
                if (lower.contains(signature)) {
                    return true;
                }
            }
        }
        return false;
    }
}
