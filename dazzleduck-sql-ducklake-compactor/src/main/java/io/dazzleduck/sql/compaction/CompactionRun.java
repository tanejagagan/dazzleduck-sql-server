package io.dazzleduck.sql.compaction;

import java.time.Instant;

/**
 * One per-cycle telemetry record for a {@code (database, tier)} compaction run — the unit defined by
 * COMPACTION_TELEMETRY_SPEC.md. Capture only: assembling one adds no catalog round-trips beyond the
 * counts the compactor already ran (the band-size query now returns {@code SUM(file_size_bytes)}
 * alongside the {@code COUNT} it already issued).
 *
 * <p>Nullable ({@link Long}) fields are values that can be genuinely unknown for a cycle — a metadata
 * count that failed to read, or a merge result the engine did not surface — and must stay distinct
 * from a real zero. {@code -1} on the {@code long} resource fields means "not measurable here"
 * (e.g. {@code /proc} is Linux-only; a tier with no {@code memory_limit} set).
 *
 * <p><b>Deviation from the spec, by design.</b> {@code durationCommitMs} is {@code -1}: the compactor
 * runs {@code CALL ducklake_merge_adjacent_files(...)} as a single autocommit statement, so DuckLake
 * performs the merge and the catalog commit inside one call with no separate JDBC commit to time.
 * {@code durationMergeMs} is the whole {@code execute()} (merge + its internal commit). Splitting the
 * two would require switching the connection to manual commit — a behaviour change the spec's
 * "capture only" scope forbids.
 */
public record CompactionRun(
        long runId,
        String database,
        String tierName,
        Instant scheduledAt,
        Instant startedAt,
        Instant endedAt,
        long intendedDelayMs,
        long actualGapMs,
        Long bandFilesBefore,
        Long bandFilesAfter,
        Long filesRetired,
        Long bandBytesBefore,
        Long bandBytesAfter,
        long groupsRequested,
        Long groupsMerged,
        long durationTotalMs,
        long durationMergeMs,
        long durationCommitMs,
        Outcome outcome,
        FailureClass failureClass,
        String errorMessage,
        long rssPeakBytes,
        long spillPeakBytes,
        long memoryLimitBytes,
        long commitTimeoutMs) {

    /** Postgres server default for {@code idle_in_transaction_session_timeout} (spec Q3). */
    public static final long DEFAULT_COMMIT_TIMEOUT_MS = 120_000;

    public enum Outcome { SUCCESS, EMPTY, FAILED }

    /**
     * Failure classes from the spec's taxonomy. Different classes demand opposite controller
     * responses (shrink the batch for OUT_OF_MEMORY / SPILL_EXCEEDED / COMMIT_TIMEOUT; do NOT shrink
     * for TRANSACTION_CONFLICT — retry; back off for CATALOG_UNAVAILABLE), so they are never collapsed.
     * {@link #NONE} is the non-failure sentinel.
     */
    public enum FailureClass { NONE, COMMIT_TIMEOUT, TRANSACTION_CONFLICT, OUT_OF_MEMORY, SPILL_EXCEEDED, CATALOG_UNAVAILABLE, OTHER }

    private static final int MAX_ERROR_LEN = 200;

    /** Classifies an exception (and its cause chain) into the spec's taxonomy by message shape. */
    public static FailureClass classify(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String m = c.getMessage();
            if (m == null) {
                continue;
            }
            String lower = m.toLowerCase(java.util.Locale.ROOT);
            // COMMIT_TIMEOUT: cycle outran idle_in_transaction_session_timeout — commit dies on a dead
            // connection, surfacing as a failed ROLLBACK. Batch too large.
            if (m.contains("Failed to execute query \"ROLLBACK\"") || lower.contains("idle_in_transaction")) {
                return FailureClass.COMMIT_TIMEOUT;
            }
            if (lower.contains("transaction conflict")) {
                return FailureClass.TRANSACTION_CONFLICT;
            }
            if (lower.contains("out of memory")) {
                return FailureClass.OUT_OF_MEMORY;
            }
            if (lower.contains("failed to offload data block") || lower.contains("max_temp_directory_size")
                    || lower.contains("temp directory")) {
                return FailureClass.SPILL_EXCEEDED;
            }
            if (lower.contains("connection refused") || lower.contains("connection reset")
                    || lower.contains("could not connect") || lower.contains("network")) {
                return FailureClass.CATALOG_UNAVAILABLE;
            }
        }
        return FailureClass.OTHER;
    }

    /** Root-cause message, truncated to ~200 chars for storage. */
    public static String truncateError(Throwable t) {
        Throwable root = t;
        for (Throwable c = t; c != null; c = c.getCause()) {
            root = c;
        }
        String m = root.getMessage() != null ? root.getMessage() : root.getClass().getName();
        return m.length() > MAX_ERROR_LEN ? m.substring(0, MAX_ERROR_LEN) + "…" : m;
    }

    /** Bytes retired = before - after, only when both band-byte reads succeeded; else null. */
    public Long bytesRetired() {
        return (bandBytesBefore != null && bandBytesAfter != null)
                ? bandBytesBefore - bandBytesAfter : null;
    }
}
