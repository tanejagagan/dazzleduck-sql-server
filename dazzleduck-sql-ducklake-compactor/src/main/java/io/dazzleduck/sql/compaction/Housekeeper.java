package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;

public interface Housekeeper extends Closeable {
    /** Expire old snapshots and delete orphaned S3 files. */
    void housekeep(String database) throws Exception;

    @Override
    default void close() throws IOException {
    }
}
