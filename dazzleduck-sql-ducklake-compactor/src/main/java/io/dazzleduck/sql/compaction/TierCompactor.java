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
     * @param filesProcessed  input files the merge compacted away ({@code SUM(files_processed)}), or
     *                        {@code null} if nothing was merged
     * @param filesCreated    new output files the merge produced ({@code SUM(files_created)}), or
     *                        {@code null} if nothing was merged
     */
    record MergeOutcome(long durationMergeMs, Long filesProcessed, Long filesCreated) {}

    @Override
    default void close() throws IOException {
    }
}
