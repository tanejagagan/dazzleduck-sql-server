package io.dazzleduck.sql.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final MajorCompactor majorCompactor;
    private final CompactionState state;

    // Shared schedulers — one task submitted per database so each runs independently
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;

    // Tracks when major compaction last ran per database
    private final ConcurrentHashMap<String, AtomicLong> lastMajorRun = new ConcurrentHashMap<>();

    public CompactionService(CompactionConfig config, MajorCompactor majorCompactor, CompactionState state) {
        this.config = config;
        this.majorCompactor = majorCompactor;
        this.state = state;
        int dbCount = Math.max(1, config.databases().size());
        this.compactionScheduler = Executors.newScheduledThreadPool(dbCount, r -> {
            Thread t = new Thread(r, "compaction");
            t.setDaemon(false);
            return t;
        });
        this.housekeepingScheduler = Executors.newScheduledThreadPool(dbCount, r -> {
            Thread t = new Thread(r, "housekeeping");
            t.setDaemon(false);
            return t;
        });
        config.databases().forEach(db -> lastMajorRun.put(db, new AtomicLong(System.currentTimeMillis())));
    }

    public void start() {
        if (config.databases().isEmpty()) {
            logger.warn("No databases configured — compaction service idle");
            return;
        }
        long minorSeconds = config.minorCompactionFrequency().toSeconds();
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();

        for (String db : config.databases()) {
            compactionScheduler.scheduleWithFixedDelay(
                    () -> runCompaction(db), 0, minorSeconds, TimeUnit.SECONDS);
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
        }

        logger.info("Compaction service started for {} database(s) — minor every {}s, major every {}s, housekeeping every {}s",
                config.databases().size(), minorSeconds, config.majorCompactionFrequency().toSeconds(), housekeepingSeconds);
    }

    public CompactionStats getStats() {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        CompactionStats base = state.getSnapshot(config.databases());
        for (Map.Entry<String, CompactionStats.DatabaseStats> entry : base.databases().entrySet()) {
            String db = entry.getKey();
            CompactionStats.DatabaseStats ds = entry.getValue();
            Instant last = state.getLastExecutionTime(db);
            Instant next = last != null ? last.plus(config.minorCompactionFrequency()) : null;
            dbStats.put(db, new CompactionStats.DatabaseStats(
                    ds.totalMinorCompactions(),
                    ds.totalMajorCompactions(),
                    ds.totalFilesCompacted(),
                    last,
                    next,
                    ds.currentSmallFiles(),
                    ds.currentMediumFiles(),
                    ds.currentTotalFiles()));
        }
        return new CompactionStats(base.serviceStart(), dbStats);
    }

    void runCompaction(String database) {
        try {
            boolean runMajor = System.currentTimeMillis() - lastMajorRun.get(database).get()
                    >= config.majorCompactionFrequency().toMillis();

            long filesBefore = queryTotalFiles(database);
            if (runMajor) {
                runMajor(database);
                state.incrementMajor(database);
                lastMajorRun.get(database).set(System.currentTimeMillis());
            } else {
                runMinor(database);
                state.incrementMinor(database);
            }
            updateFileCounts(database);
            long filesAfter = queryTotalFiles(database);
            state.addFilesCompacted(database, filesBefore - filesAfter);
            state.recordLastExecution(database);
        } catch (Throwable t) {
            logger.error("Unexpected error in compaction cycle for {} — scheduler will continue", database, t);
        }
    }

    void runHousekeeping(String database) {
        try {
            majorCompactor.housekeep(database);
            logger.info("Housekeeping completed for {}", database);
        } catch (Throwable t) {
            logger.error("Unexpected error in housekeeping cycle for {} — scheduler will continue", database, t);
        }
    }

    private void runMinor(String database) {
        Timer.Sample sample = state.startTimer();
        try (var connection = ConnectionPool.getConnection()) {
            String sql = "CALL ducklake_merge_adjacent_files('%s', max_file_size := %d)"
                    .formatted(database, config.minorCompactionMaxSize());
            ConnectionPool.execute(connection, sql);
            logger.info("Minor compaction completed for {}", database);
        } catch (Exception e) {
            logger.error("Minor compaction failed for {}", database, e);
        } finally {
            state.stopTimer(sample, "minor", "merge", database);
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

    private long queryTotalFiles(String database) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = "SELECT COUNT(*) AS total FROM %s.ducklake_data_file WHERE end_snapshot IS NULL"
                .formatted(mdDatabase);
        try (var connection = ConnectionPool.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                return rs.next() ? rs.getLong("total") : 0L;
            }
        } catch (Exception e) {
            logger.warn("Could not query file count for {}", database, e);
            return 0L;
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
                    state.updateFileCounts(database,
                            rs.getLong("small_files"),
                            rs.getLong("medium_files"),
                            rs.getLong("total_files"));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to update file counts for {}", database, e);
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
