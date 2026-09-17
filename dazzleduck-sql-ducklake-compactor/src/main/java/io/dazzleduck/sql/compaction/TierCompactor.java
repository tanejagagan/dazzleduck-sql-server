package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;

public interface TierCompactor extends Closeable {
    /** Merge small Parquet files into larger ones within the given tier's file-size range. */
    void compact(String database, CompactionTier tier) throws Exception;

    @Override
    default void close() throws IOException {
    }
}
