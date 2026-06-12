package io.dazzleduck.sql.compaction;

public interface MajorCompactor {
    /** Merge small Parquet files into larger ones within the catalog. */
    void compact(String database) throws Exception;

    /** Expire old snapshots and delete orphaned S3 files. */
    void housekeep(String database) throws Exception;
}
