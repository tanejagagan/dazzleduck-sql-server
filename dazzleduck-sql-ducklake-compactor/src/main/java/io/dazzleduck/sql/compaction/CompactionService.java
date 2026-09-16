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
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final MajorCompactor majorCompactor;
    private final CompactionState state;

    // Shared schedulers
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;

    // Per-cycle-kind last-run tracking (in addition to CompactionState's single shared one), so
    // getStats() can report each cycle's own next-due time instead of a conflated one. Stamped with
    // the exact same Instant passed to CompactionState.recordRunCompleted(db, at) in the same call,
    // so the two never disagree by a few nanoseconds.
    private final ConcurrentHashMap<String, Instant> lastMinorRun = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Instant> lastMajorRun = new ConcurrentHashMap<>();

    public CompactionService(CompactionConfig config, MajorCompactor majorCompactor, CompactionState state) {
        this.config = config;
        this.majorCompactor = majorCompactor;
        this.state = state;
        // One task submitted per database per enabled cycle (minor and major are independent
        // schedules that can run concurrently), so the pool needs a thread for each, not just one
        // per database — undersizing this would silently serialize minor and major onto whichever
        // threads are free, defeating the point of decoupling them.
        int tasksPerDb = (config.minorCompactionEnabled() ? 1 : 0) + (config.majorCompactionEnabled() ? 1 : 0);
        int compactionThreads = Math.max(1, config.databases().size() * Math.max(tasksPerDb, 1));
        int housekeepingThreads = Math.max(1, config.databases().size());
        this.compactionScheduler = Executors.newScheduledThreadPool(compactionThreads, r -> {
            Thread t = new Thread(r, "compaction");
            t.setDaemon(false);
            return t;
        });
        this.housekeepingScheduler = Executors.newScheduledThreadPool(housekeepingThreads, r -> {
            Thread t = new Thread(r, "housekeeping");
            t.setDaemon(false);
            return t;
        });
    }

    public void start() {
        if (config.databases().isEmpty()) {
            logger.warn("No databases configured — compaction service idle");
            return;
        }
        long minorSeconds = config.minorCompactionFrequency().toSeconds();
        long majorSeconds = config.majorCompactionFrequency().toSeconds();
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();

        for (String db : config.databases()) {
            // Minor and major are independent schedules now — each fires on its own cadence and, when
            // both enabled, run concurrently. That's safe only because CompactionConfig validated at
            // startup that their file-size ranges are disjoint (min_file_size/max_file_size fencing in
            // DuckDbMajorCompactor), so no lock is needed here.
            //
            // Fixed-RATE, not fixed-delay: successive runs are due at fixed_rate, 2*fixed_rate, ...
            // from the start of scheduling, so a run that took time T waits (interval - T) before the
            // next one starts, rather than a full interval after completion regardless of T. If a run
            // takes longer than the interval, the next one starts immediately with no negative wait —
            // scheduleAtFixedRate's normal saturation behavior. Both runMinor/runMajor already catch
            // every Throwable internally, so a failing cycle never suppresses subsequent scheduled runs
            // (a Runnable that escapes with an exception is scheduleAtFixedRate's one failure mode).
            if (config.minorCompactionEnabled()) {
                compactionScheduler.scheduleAtFixedRate(
                        () -> runMinor(db), 0, minorSeconds, TimeUnit.SECONDS);
            }
            if (config.majorCompactionEnabled()) {
                compactionScheduler.scheduleAtFixedRate(
                        () -> runMajor(db), 0, majorSeconds, TimeUnit.SECONDS);
            }
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
        }

        logger.info("Compaction service started for {} database(s) — minor {}every {}s, major {}every {}s, housekeeping every {}s",
                config.databases().size(),
                config.minorCompactionEnabled() ? "" : "(disabled) ", minorSeconds,
                config.majorCompactionEnabled() ? "" : "(disabled) ", majorSeconds,
                housekeepingSeconds);
    }

    public CompactionStats getStats() {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        CompactionStats base = state.getSnapshot(config.databases());
        base.databases().forEach((db, ds) -> dbStats.put(db, ds.withNextExecutionTime(nextExecutionTime(db))));
        return new CompactionStats(base.serviceStart(), dbStats);
    }

    /**
     * Minor and major tick independently now, so there's no single shared cadence to report a
     * next-execution time against — this is the earlier of each cycle's own last completion plus its
     * own frequency (only for whichever is enabled), not a single shared timestamp. Null when neither
     * is enabled, or enabled but has not completed a cycle yet.
     */
    private Instant nextExecutionTime(String database) {
        Instant next = null;
        if (config.minorCompactionEnabled()) {
            Instant lastMinor = lastMinorRun.get(database);
            if (lastMinor != null) {
                next = lastMinor.plus(config.minorCompactionFrequency());
            }
        }
        if (config.majorCompactionEnabled()) {
            Instant lastMajor = lastMajorRun.get(database);
            if (lastMajor != null) {
                Instant majorNext = lastMajor.plus(config.majorCompactionFrequency());
                next = (next == null || majorNext.isBefore(next)) ? majorNext : next;
            }
        }
        return next;
    }

    void runMinor(String database) {
        // Keeping the whole body inside the try is what prevents a stray throwable from escaping the
        // scheduled task — a Runnable that throws would cancel this database's minor compaction
        // forever under scheduleAtFixedRate.
        try {
            OptionalLong filesBefore = queryFileCount(database, null, config.minorCompactionMaxSize());
            runMinorMerge(database);
            state.incrementMinor(database);
            recordFileDelta(database, filesBefore, null, config.minorCompactionMaxSize());
            state.recordSuccess(database);
        } catch (Throwable t) {
            state.recordFailure(database, CycleKind.MINOR);
            logger.error("Minor compaction cycle failed for {} — scheduler will continue", database, t);
        } finally {
            // Stamp every cycle's completion, success or failure, so /health can report when the
            // scheduler will run this database again. The same instant feeds both trackers so they
            // never disagree with each other.
            Instant now = Instant.now();
            lastMinorRun.put(database, now);
            state.recordRunCompleted(database, now);
        }
    }

    void runMajor(String database) {
        try {
            OptionalLong filesBefore = queryFileCount(database, config.minorCompactionMaxSize(), config.majorCompactionMaxSize());
            majorCompactor.compact(database);
            logger.info("Major compaction completed for {}", database);
            state.incrementMajor(database);
            recordFileDelta(database, filesBefore, config.minorCompactionMaxSize(), config.majorCompactionMaxSize());
            state.recordSuccess(database);
        } catch (Throwable t) {
            state.recordFailure(database, CycleKind.MAJOR);
            logger.error("Major compaction cycle failed for {} — scheduler will continue", database, t);
        } finally {
            Instant now = Instant.now();
            lastMajorRun.put(database, now);
            state.recordRunCompleted(database, now);
        }
    }

    /**
     * Only records a delta when both the before and after reads succeeded; a failed metadata read
     * must not be treated as "zero files" or the cumulative counter is permanently inflated.
     *
     * <p>The before/after counts are scoped to this cycle's own file-size range (matching the bounds
     * its merge call itself used), not the whole catalog — minor and major can run concurrently now,
     * and an unscoped whole-catalog count would let one attribute the other's file reduction to
     * itself (or double-count the same reduction across both). {@link #updateFileCounts} still
     * refreshes the whole-catalog gauges separately, which is a deliberately global, point-in-time
     * snapshot rather than a delta.
     */
    private void recordFileDelta(String database, OptionalLong filesBefore, Long minSizeInclusive, long maxSizeExclusive) {
        updateFileCounts(database);
        OptionalLong filesAfter = queryFileCount(database, minSizeInclusive, maxSizeExclusive);
        if (filesBefore.isPresent() && filesAfter.isPresent()) {
            state.addFilesCompacted(database, filesBefore.getAsLong() - filesAfter.getAsLong());
        }
    }

    void runHousekeeping(String database) {
        try {
            majorCompactor.housekeep(database);
            logger.info("Housekeeping completed for {}", database);
        } catch (Throwable t) {
            state.recordFailure(database, CycleKind.HOUSEKEEPING);
            logger.error("Unexpected error in housekeeping cycle for {} — scheduler will continue", database, t);
        }
    }

    private void runMinorMerge(String database) throws Exception {
        Timer.Sample sample = state.startTimer();
        try (var connection = ConnectionPool.getConnection(config.minorConnectionSettings())) {
            // Unbounded (max_compacted_files := 0, the default) merges every eligible file across
            // every table in the catalog in a single call — on a large catalog this can hold open a
            // transaction whose native memory footprint grows with the whole database rather than
            // with minor_compaction_max_size, and a cycle that never returns never reports a
            // completed "minor"/"merge" duration either. Capping it turns one unbounded pass into
            // several bounded ones: this tick merges up to minorCompactionMaxFiles files and the next
            // scheduled tick (minor_compaction_frequency later) picks up where it left off. No lower
            // bound (null) — minor has no min_file_size, unlike major.
            String sql = DuckDbMajorCompactor.mergeAdjacentFilesSql(
                    database, null, config.minorCompactionMaxSize(), config.minorCompactionMaxFiles());
            ConnectionPool.execute(connection, sql);
            logger.info("Minor compaction completed for {}", database);
        } finally {
            state.stopTimer(sample, "minor", "merge", database);
        }
    }

    /**
     * Counts live files (`end_snapshot IS NULL`) in {@code [minSizeInclusive, maxSizeExclusive)} —
     * {@code minSizeInclusive} of {@code null} means no lower bound. Scoped to match exactly the
     * range a cycle's own merge call touches, so minor and major (which run concurrently) each only
     * ever measure their own range — see {@link #recordFileDelta}.
     */
    private OptionalLong queryFileCount(String database, Long minSizeInclusive, long maxSizeExclusive) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String rangeFilter = minSizeInclusive != null
                ? "file_size_bytes >= %d AND file_size_bytes < %d".formatted(minSizeInclusive, maxSizeExclusive)
                : "file_size_bytes < %d".formatted(maxSizeExclusive);
        String sql = "SELECT COUNT(*) AS total FROM %s.ducklake_data_file WHERE end_snapshot IS NULL AND %s"
                .formatted(mdDatabase, rangeFilter);
        try (var connection = ConnectionPool.getConnection();
             var statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                return rs.next() ? OptionalLong.of(rs.getLong("total")) : OptionalLong.empty();
            }
        } catch (Exception e) {
            logger.warn("Could not query file count for {}", database, e);
            return OptionalLong.empty();
        }
    }

    /**
     * Refreshes the whole-catalog file-count gauges. Deliberately a global point-in-time snapshot,
     * not scoped to either cycle's own range — unlike {@link #queryFileCount}, which backs the
     * per-cycle delta, this backs the small/medium/total gauges operators watch for the catalog as a
     * whole, which is meaningful regardless of which cycle happened to trigger the refresh.
     */
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
