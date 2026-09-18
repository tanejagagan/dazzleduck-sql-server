package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;

public interface TierCompactor extends Closeable {
    /**
     * Merge small Parquet files into larger ones within the given tier's file-size range, returning
     * per-cycle timing/telemetry for the merge.
     */
    MergeOutcome compact(String database, CompactionTier tier) throws Exception;

    /**
     * Timing of one merge call. There is no separate commit time — DuckLake merges and commits inside
     * the single CALL (see {@link CompactionRun}), so {@code durationMergeMs} covers both.
     *
     * @param durationMergeMs wall-clock ms of {@code CALL ducklake_merge_adjacent_files(...)}
     * @param groupsMerged    merge groups the engine reported, or {@code null} if it surfaces none
     */
    record MergeOutcome(long durationMergeMs, Long groupsMerged) {}

    @Override
    default void close() throws IOException {
    }
}
