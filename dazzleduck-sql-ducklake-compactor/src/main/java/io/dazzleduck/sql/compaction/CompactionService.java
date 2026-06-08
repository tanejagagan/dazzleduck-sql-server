package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final MajorCompactor majorCompactor;
    private final CompactionMetrics metrics;
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;
    private final AtomicLong lastMajorRun = new AtomicLong(System.currentTimeMillis());

    public CompactionService(CompactionConfig config, MajorCompactor majorCompactor, CompactionMetrics metrics) {
        this.config = config;
        this.majorCompactor = majorCompactor;
        this.metrics = metrics;
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "compaction-scheduler");
            t.setDaemon(false);
            return t;
        });
        this.housekeepingScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "housekeeping-scheduler");
            t.setDaemon(false);
            return t;
        });
    }

    public void start() {
        long minorSeconds = config.minorCompactionFrequency().toSeconds();
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();

        compactionScheduler.scheduleWithFixedDelay(this::runCompaction, 0, minorSeconds, TimeUnit.SECONDS);
        housekeepingScheduler.scheduleWithFixedDelay(this::runHousekeeping, housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);

        logger.info("Compaction service started — minor every {}s, major every {}s, housekeeping every {}s",
                minorSeconds, config.majorCompactionFrequency().toSeconds(), housekeepingSeconds);
    }

    void runCompaction() {
        try {
            List<String> databases = config.databases();
            if (databases.isEmpty()) {
                logger.warn("No databases configured, skipping compaction");
                return;
            }

            boolean runMajor = System.currentTimeMillis() - lastMajorRun.get()
                    >= config.majorCompactionFrequency().toMillis();

            for (String database : databases) {
                if (runMajor) {
                    runMajor(database);
                } else {
                    runMinor(database);
                }
                updateFileCounts(database);
            }

            if (runMajor) {
                lastMajorRun.set(System.currentTimeMillis());
            }
        } catch (Throwable t) {
            logger.error("Unexpected error in compaction cycle — scheduler will continue", t);
        }
    }

    void runHousekeeping() {
        try {
            List<String> databases = config.databases();
            if (databases.isEmpty()) return;

            for (String database : databases) {
                try {
                    majorCompactor.housekeep(database);
                    logger.info("Housekeeping completed for {}", database);
                } catch (Exception e) {
                    logger.error("Housekeeping failed for {}", database, e);
                }
            }
        } catch (Throwable t) {
            logger.error("Unexpected error in housekeeping cycle — scheduler will continue", t);
        }
    }

    private void runMinor(String database) {
        Timer.Sample sample = null;
        try (var connection = ConnectionPool.getConnection()) {
            sample = metrics.startTimer();
            String sql = "CALL ducklake_merge_adjacent_files('%s', max_file_size := %d)"
                    .formatted(database, config.minorCompactionMaxSize());
            ConnectionPool.execute(connection, sql);
            logger.info("Minor compaction completed for {}", database);
        } catch (Exception e) {
            logger.error("Minor compaction failed for {}", database, e);
        } finally {
            if (sample != null) metrics.stopTimer(sample, "minor", "merge", database);
        }
    }

    private void runMajor(String database) {
        try {
            majorCompactor.compact(database);
            logger.info("Major compaction completed for {}", database);
        } catch (Exception e) {
            logger.error("Major compaction failed for {}", database, e);
        }
    }

    private void updateFileCounts(String database) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = """
                SELECT
                  COUNT(*) FILTER (WHERE file_size_bytes < %d) AS small_files,
                  COUNT(*) FILTER (WHERE file_size_bytes >= %d AND file_size_bytes < %d) AS medium_files,
                  COUNT(*) AS total_files
                FROM %s.ducklake_data_file
                WHERE end_snapshot IS NULL
                """.formatted(
                config.minorCompactionMaxSize(),
                config.minorCompactionMaxSize(),
                config.majorCompactionMaxSize(),
                mdDatabase);

        try (var connection = ConnectionPool.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                if (rs.next()) {
                    metrics.updateFileCounts(database,
                            rs.getLong("small_files"),
                            rs.getLong("medium_files"),
                            rs.getLong("total_files"));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to update file count metrics for {}", database, e);
        }
    }

    @Override
    public void close() {
        compactionScheduler.shutdown();
        housekeepingScheduler.shutdown();
        try {
            if (!compactionScheduler.awaitTermination(30, TimeUnit.SECONDS))
                compactionScheduler.shutdownNow();
            if (!housekeepingScheduler.awaitTermination(30, TimeUnit.SECONDS))
                housekeepingScheduler.shutdownNow();
        } catch (InterruptedException e) {
            compactionScheduler.shutdownNow();
            housekeepingScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Compaction service stopped");
    }
}
