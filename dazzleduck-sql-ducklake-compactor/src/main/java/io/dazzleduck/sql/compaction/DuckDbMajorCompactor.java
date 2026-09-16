package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class DuckDbMajorCompactor implements MajorCompactor {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbMajorCompactor.class);

    private final long minFileSizeBytes;
    private final long maxFileSizeBytes;
    private final long maxCompactedFiles;
    private final Duration snapshotRetention;
    private final List<String> connectionSettings;
    private final CompactionState metrics;

    public DuckDbMajorCompactor(long minFileSizeBytes, long maxFileSizeBytes, long maxCompactedFiles,
                                 Duration snapshotRetention, List<String> connectionSettings, CompactionState metrics) {
        this.minFileSizeBytes = minFileSizeBytes;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.maxCompactedFiles = maxCompactedFiles;
        this.snapshotRetention = snapshotRetention;
        this.connectionSettings = connectionSettings;
        this.metrics = metrics;
    }

    @Override
    public void compact(String database) throws Exception {
        try (var connection = ConnectionPool.getConnection(connectionSettings)) {
            String sql = mergeAdjacentFilesSql(database, minFileSizeBytes, maxFileSizeBytes, maxCompactedFiles);
            time("major", "merge", database, () -> ConnectionPool.execute(connection, sql));
        }
    }

    /**
     * Shared with {@link CompactionService#runMinorMerge}, which calls this with a null
     * {@code minFileSizeBytes} (minor has no lower bound). {@code min_file_size} is what fences
     * major away from minor's range — the two run concurrently with no lock, safe only as long as
     * their ranges stay disjoint (enforced at startup in {@link CompactionConfig#from}).
     * {@code max_compacted_files} is appended only when positive, matching that parameter's own
     * unbounded-at-zero convention.
     */
    static String mergeAdjacentFilesSql(String database, Long minFileSizeBytes, long maxFileSizeBytes, long maxCompactedFiles) {
        StringBuilder sql = new StringBuilder("CALL ducklake_merge_adjacent_files('%s'".formatted(database));
        if (minFileSizeBytes != null) {
            sql.append(", min_file_size := ").append(minFileSizeBytes);
        }
        sql.append(", max_file_size := ").append(maxFileSizeBytes);
        if (maxCompactedFiles > 0) {
            sql.append(", max_compacted_files := ").append(maxCompactedFiles);
        }
        return sql.append(")").toString();
    }

    @Override
    public void housekeep(String database) throws Exception {
        long retentionSeconds = snapshotRetention.toSeconds();

        try (var connection = ConnectionPool.getConnection(connectionSettings)) {
            // Run steps independently so a failure in expire does not silently skip cleanup
            try {
                time("housekeeping", "expire", database, () -> ConnectionPool.execute(connection,
                        "CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')"
                                .formatted(database, retentionSeconds)));
            } catch (Exception e) {
                logger.error("Snapshot expiry failed for {}, cleanup will still run", database, e);
            }
            time("housekeeping", "cleanup", database, () -> ConnectionPool.execute(connection,
                    "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                            .formatted(database, retentionSeconds)));
        }
    }

    private void time(String type, String step, String database, Runnable action) {
        Timer.Sample sample = metrics.startTimer();
        try {
            action.run();
        } finally {
            metrics.stopTimer(sample, type, step, database);
        }
        logger.debug("{} step '{}' completed for {}", type, step, database);
    }
}
