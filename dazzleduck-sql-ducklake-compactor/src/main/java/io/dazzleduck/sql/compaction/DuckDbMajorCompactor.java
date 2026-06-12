package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class DuckDbMajorCompactor implements MajorCompactor {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbMajorCompactor.class);

    private final long maxFileSizeBytes;
    private final Duration snapshotRetention;
    private final CompactionMetrics metrics;

    public DuckDbMajorCompactor(long maxFileSizeBytes, Duration snapshotRetention, CompactionMetrics metrics) {
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.snapshotRetention = snapshotRetention;
        this.metrics = metrics;
    }

    @Override
    public void compact(String database) throws Exception {
        try (var connection = ConnectionPool.getConnection()) {
            time("major", "merge", database, () -> ConnectionPool.execute(connection,
                    "CALL ducklake_merge_adjacent_files('%s', max_file_size := %d)"
                            .formatted(database, maxFileSizeBytes)));
        }
    }

    @Override
    public void housekeep(String database) throws Exception {
        long retentionSeconds = snapshotRetention.toSeconds();

        try (var connection = ConnectionPool.getConnection()) {
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
