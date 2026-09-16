package io.dazzleduck.sql.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionService implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionService.class);

    private final CompactionConfig config;
    private final String startupScript;
    private final TierCompactor tierCompactor;
    private final Housekeeper housekeeper;
    private final CompactionState state;

    // Shared schedulers
    private final ScheduledExecutorService compactionScheduler;
    private final ScheduledExecutorService housekeepingScheduler;

    // database -> tier name -> last-run instant. Stamped with the exact same Instant passed to
    // CompactionState.recordRunCompleted(db, at) in the same call, so the two never disagree.
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Instant>> lastTierRun = new ConcurrentHashMap<>();

    // One raw connection per (database, tier) for this service's own file-count queries —
    // deliberately not io.dazzleduck.sql.commons.ConnectionPool (see RawConnections). Keyed by both,
    // not database alone: different tiers for the same database run concurrently by design, and a
    // plain JDBC Connection is not safe for concurrent use by more than one thread — the same reason
    // DuckDbTierCompactor/DuckLakeHousekeeper key their own connections this way.
    private final ConcurrentHashMap<String, Connection> countingConnections = new ConcurrentHashMap<>();

    public CompactionService(CompactionConfig config, String startupScript, TierCompactor tierCompactor, Housekeeper housekeeper, CompactionState state) {
        this.config = config;
        this.startupScript = startupScript;
        this.tierCompactor = tierCompactor;
        this.housekeeper = housekeeper;
        this.state = state;
        // One task submitted per database per enabled tier — tiers are independent schedules that
        // can run concurrently (safe because CompactionConfig validated their file-size ranges are
        // disjoint), so the pool needs a thread for each, not just one per database. Undersizing this
        // would silently serialize tiers onto whichever threads are free, defeating the point of
        // decoupling them.
        long enabledTiers = config.tiers().stream().filter(CompactionTier::enabled).count();
        int compactionThreads = Math.max(1, (int) (config.databases().size() * Math.max(enabledTiers, 1)));
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
        long housekeepingSeconds = config.housekeepingFrequency().toSeconds();

        for (String db : config.databases()) {
            // Each enabled tier gets its own independent schedule and, when more than one is
            // enabled, they run concurrently. That's safe only because CompactionConfig validated at
            // startup that every enabled tier's file-size range is disjoint from every other's
            // (min_file_size/max_file_size fencing in DuckDbTierCompactor), so no lock is needed here.
            //
            // Fixed-RATE, not fixed-delay: successive runs are due at fixed_rate, 2*fixed_rate, ...
            // from the start of scheduling, so a run that took time T waits (interval - T) before the
            // next one starts, rather than a full interval after completion regardless of T. If a run
            // takes longer than the interval, the next one starts immediately with no negative wait —
            // scheduleAtFixedRate's normal saturation behavior. runTier already catches every
            // Throwable internally, so a failing cycle never suppresses subsequent scheduled runs (a
            // Runnable that escapes with an exception is scheduleAtFixedRate's one failure mode).
            for (CompactionTier tier : config.tiers()) {
                if (tier.enabled()) {
                    compactionScheduler.scheduleAtFixedRate(
                            () -> runTier(db, tier), 0, tier.frequency().toSeconds(), TimeUnit.SECONDS);
                }
            }
            housekeepingScheduler.scheduleWithFixedDelay(
                    () -> runHousekeeping(db), housekeepingSeconds, housekeepingSeconds, TimeUnit.SECONDS);
        }

        logger.info("Compaction service started for {} database(s), {} tier(s) ({}), housekeeping every {}s",
                config.databases().size(), config.tiers().size(),
                config.tiers().stream()
                        .map(t -> t.name() + (t.enabled() ? "" : " (disabled)"))
                        .reduce((a, b) -> a + ", " + b).orElse(""),
                housekeepingSeconds);
    }

    public CompactionStats getStats() {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        CompactionStats base = state.getSnapshot(config.databases());
        base.databases().forEach((db, ds) -> dbStats.put(db, ds.withNextExecutionTimeByTier(nextExecutionTimeByTier(db))));
        return new CompactionStats(base.serviceStart(), dbStats);
    }

    /**
     * Each tier ticks independently now, so there's no single shared cadence to report a
     * next-execution time against — one entry per enabled tier, each its own last completion plus
     * its own frequency. A tier that hasn't completed a cycle yet is omitted.
     */
    private Map<String, Instant> nextExecutionTimeByTier(String database) {
        Map<String, Instant> next = new HashMap<>();
        Map<String, Instant> lastRuns = lastTierRun.getOrDefault(database, new ConcurrentHashMap<>());
        for (CompactionTier tier : config.tiers()) {
            if (!tier.enabled()) {
                continue;
            }
            Instant lastRun = lastRuns.get(tier.name());
            if (lastRun != null) {
                next.put(tier.name(), lastRun.plus(tier.frequency()));
            }
        }
        return next;
    }

    void runTier(String database, CompactionTier tier) {
        // Keeping the whole body inside the try is what prevents a stray throwable from escaping the
        // scheduled task — a Runnable that throws would cancel this tier's schedule forever under
        // scheduleAtFixedRate.
        try {
            OptionalLong filesBefore = queryFileCount(database, tier.name(), tier.minFileSize(), tier.maxFileSize());
            tierCompactor.compact(database, tier);
            logger.info("Tier '{}' compaction completed for {}", tier.name(), database);
            state.incrementTier(database, tier.name());
            recordFileDelta(database, tier.name(), filesBefore, tier.minFileSize(), tier.maxFileSize());
            state.recordSuccess(database);
        } catch (Throwable t) {
            state.recordFailure(database, tier.name());
            logger.error("Tier '{}' compaction cycle failed for {} — scheduler will continue", tier.name(), database, t);
        } finally {
            // Stamp every cycle's completion, success or failure, so /health can report when the
            // scheduler will run this database's tier again. The same instant feeds both trackers so
            // they never disagree with each other.
            Instant now = Instant.now();
            lastTierRun.computeIfAbsent(database, k -> new ConcurrentHashMap<>()).put(tier.name(), now);
            state.recordRunCompleted(database, now);
        }
    }

    /**
     * Only records a delta when both the before and after reads succeeded; a failed metadata read
     * must not be treated as "zero files" or the cumulative counter is permanently inflated.
     *
     * <p>The before/after counts are scoped to this tier's own file-size range (matching the bounds
     * its merge call itself used), not the whole catalog — tiers can run concurrently now, and an
     * unscoped whole-catalog count would let one attribute another's file reduction to itself (or
     * double-count the same reduction across several tiers). {@link #updateAllTierFileCounts} still
     * refreshes every tier's whole-catalog gauges separately, which is a deliberately global,
     * point-in-time snapshot rather than a delta.
     */
    private void recordFileDelta(String database, String tierName, OptionalLong filesBefore, long minSizeInclusive, long maxSizeExclusive) {
        updateAllTierFileCounts(database, tierName);
        OptionalLong filesAfter = queryFileCount(database, tierName, minSizeInclusive, maxSizeExclusive);
        if (filesBefore.isPresent() && filesAfter.isPresent()) {
            state.addFilesCompacted(database, filesBefore.getAsLong() - filesAfter.getAsLong());
        }
    }

    void runHousekeeping(String database) {
        try {
            housekeeper.housekeep(database);
            logger.info("Housekeeping completed for {}", database);
        } catch (Throwable t) {
            state.recordFailure(database, CompactionState.HOUSEKEEPING_KIND);
            logger.error("Unexpected error in housekeeping cycle for {} — scheduler will continue", database, t);
        }
    }

    /**
     * Counts live files (`end_snapshot IS NULL`) in `[minSizeInclusive, maxSizeExclusive)`. Scoped
     * to match exactly the range a tier's own merge call touches, so concurrently-running tiers each
     * only ever measure their own range — see {@link #recordFileDelta}.
     */
    private OptionalLong queryFileCount(String database, String tierName, long minSizeInclusive, long maxSizeExclusive) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        String sql = "SELECT COUNT(*) AS total FROM %s.ducklake_data_file WHERE end_snapshot IS NULL AND file_size_bytes >= %d AND file_size_bytes < %d"
                .formatted(mdDatabase, minSizeInclusive, maxSizeExclusive);
        try (var statement = countingConnectionFor(database, tierName).createStatement()) {
            statement.execute(sql);
            try (ResultSet rs = statement.getResultSet()) {
                return rs.next() ? OptionalLong.of(rs.getLong("total")) : OptionalLong.empty();
            }
        } catch (Exception e) {
            logger.warn("Could not query file count for {}", database, e);
            return OptionalLong.empty();
        }
    }

    private Connection countingConnectionFor(String database, String tierName) throws SQLException {
        String key = database.length() + ":" + database + "|" + tierName;
        Connection existing = countingConnections.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = countingConnections.get(key);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, List.of());
            countingConnections.put(key, opened);
            return opened;
        }
    }

    /**
     * Refreshes every configured tier's current-file-count gauge plus the whole-catalog total, in
     * one query, using the triggering tier's own counting connection. Deliberately a global
     * point-in-time snapshot of every tier, not just the one whose cycle triggered the refresh —
     * meaningful regardless of which tier happened to trigger it.
     */
    private void updateAllTierFileCounts(String database, String triggeringTierName) {
        String mdDatabase = "\"__ducklake_metadata_" + database + "\"";
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_files");
        for (CompactionTier tier : config.tiers()) {
            sql.append(", COUNT(*) FILTER (WHERE file_size_bytes >= %d AND file_size_bytes < %d) AS \"%s\""
                    .formatted(tier.minFileSize(), tier.maxFileSize(), tier.name()));
        }
        sql.append(" FROM %s.ducklake_data_file WHERE end_snapshot IS NULL".formatted(mdDatabase));

        try (var statement = countingConnectionFor(database, triggeringTierName).createStatement()) {
            statement.execute(sql.toString());
            try (ResultSet rs = statement.getResultSet()) {
                if (rs.next()) {
                    for (CompactionTier tier : config.tiers()) {
                        state.updateTierFileCount(database, tier.name(), rs.getLong(tier.name()));
                    }
                    state.updateTotalFiles(database, rs.getLong("total_files"));
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
        closeQuietly(tierCompactor);
        closeQuietly(housekeeper);
        countingConnections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a counting connection", e);
            }
        });
        logger.info("Compaction service stopped");
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Failed to close {}", closeable.getClass().getSimpleName(), e);
        }
    }
}
