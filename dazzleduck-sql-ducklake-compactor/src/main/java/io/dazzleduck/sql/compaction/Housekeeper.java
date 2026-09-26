package io.dazzleduck.sql.compaction;

import java.io.Closeable;
import java.io.IOException;

public interface Housekeeper extends Closeable {
    /** Rewrite files with many deleted rows, expire old snapshots and delete retired files. */
    void housekeep(String database) throws Exception;

    @Override
    default void close() throws IOException {
    }
}
