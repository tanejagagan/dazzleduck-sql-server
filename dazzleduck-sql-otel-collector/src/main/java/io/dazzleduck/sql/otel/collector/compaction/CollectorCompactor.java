package io.dazzleduck.sql.otel.collector.compaction;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DuckLake maintenance inside the collector, on the collector's own DuckDB instance.
 *
 * <p>Uses {@link ConnectionPool} (duplicates of the process-wide instance) rather than opening its
 * own DuckDB instance: a DuckDB-file or SQLite catalog is attached once per process, and a second
 * instance attaching it hits the file lock. The flip side is that maintenance shares the instance's
 * global {@code memory_limit} and {@code threads} with ingestion.
 *
 * <p>All jobs run on one thread, so they never overlap and cannot conflict with each other. Each
 * step is its own transaction and runs even if an earlier one failed; a lost transaction conflict
 * is retried on the next run. Ingestion only adds files, which does not conflict with merging.
 *
 * <p>The steps are explicit calls rather than {@code CHECKPOINT}, which cannot be given ages: its
 * expiry is a no-op unless the catalog sets {@code expire_older_than}, and one catalog option
 * ({@code delete_older_than}) would govern both old-file and orphaned-file deletion.
 */
public class CollectorCompactor implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CollectorCompactor.class);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final CompactionSettings settings;
    private final MeterRegistry registry;
    private final ScheduledExecutorService scheduler;
    // The statement currently executing, so close() can cancel a long merge.
    private volatile Statement running;

    public CollectorCompactor(CompactionSettings settings, MeterRegistry registry) {
        this.settings = settings;
        this.registry = registry;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otel-compaction");
            t.setDaemon(true);
            return t;
        });
    }

    /** Schedules the jobs; a no-op when compaction is disabled. */
    public void start() {
        if (!settings.enabled()) {
            return;
        }
        schedule(this::runMinor, settings.minorFrequency());
        schedule(this::runMajor, settings.majorFrequency());
        if (settings.orphanCleanupEnabled()) {
            schedule(this::runOrphanCleanup, settings.orphanFrequency());
        }
        log.info("Compaction enabled for {}: minor every {} (files < {} bytes), major every {} (retention {}), "
                        + "orphan cleanup {}",
                settings.catalogs(), settings.minorFrequency(), settings.minorMaxFileSize(),
                settings.majorFrequency(), settings.snapshotRetention(),
                settings.orphanCleanupEnabled()
                        ? "every " + settings.orphanFrequency() + " (older than " + settings.orphanOlderThan() + ")"
                        : "off");
    }

    private void schedule(Runnable job, Duration every) {
        long ms = every.toMillis();
        // Fixed delay: a slow run pushes the next one back instead of queueing runs behind it.
        scheduler.scheduleWithFixedDelay(job, ms, ms, TimeUnit.MILLISECONDS);
    }

    /** Merges files smaller than {@code minor.max_file_size}. */
    void runMinor() {
        for (String catalog : settings.catalogs()) {
            step(catalog, "minor_merge", "CALL ducklake_merge_adjacent_files('%s', max_file_size => %d)"
                    .formatted(catalog, settings.minorMaxFileSize()), "files_merged");
        }
    }

    /** Flushes inlined data, expires snapshots, merges all sizes, rewrites deletes, deletes retired files. */
    void runMajor() {
        long retentionSeconds = settings.snapshotRetention().toSeconds();
        for (String catalog : settings.catalogs()) {
            step(catalog, "flush_inlined", "CALL ducklake_flush_inlined_data('%s')".formatted(catalog), null);
            step(catalog, "expire_snapshots", "CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, retentionSeconds), null);
            step(catalog, "major_merge", "CALL ducklake_merge_adjacent_files('%s')".formatted(catalog), "files_merged");
            step(catalog, "rewrite_deletes", settings.rewriteDeleteThreshold() == null
                    ? "CALL ducklake_rewrite_data_files('%s')".formatted(catalog)
                    : "CALL ducklake_rewrite_data_files('%s', delete_threshold => %s)"
                            .formatted(catalog, settings.rewriteDeleteThreshold()), "files_rewritten");
            step(catalog, "cleanup_old_files", "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, retentionSeconds), null);
        }
    }

    /** Deletes files under the catalog's data path that it does not reference and that are old enough. */
    void runOrphanCleanup() {
        long ageSeconds = settings.orphanOlderThan().toSeconds();
        for (String catalog : settings.catalogs()) {
            step(catalog, "delete_orphaned_files", "CALL ducklake_delete_orphaned_files('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, ageSeconds), null);
        }
    }

    /**
     * Runs one step and records its duration. Never throws: a scheduled task that throws is never
     * run again. When {@code filesCounter} is set, the step's {@code files_processed} total is added
     * to that counter.
     */
    private void step(String catalog, String step, String sql, String filesCounter) {
        Timer.Sample sample = Timer.start(registry);
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement()) {
            running = statement;
            long processed = 0;
            if (statement.execute(sql) && filesCounter != null) {
                try (var rs = statement.getResultSet()) {
                    while (rs.next()) {
                        processed += rs.getLong("files_processed");
                    }
                }
            }
            if (processed > 0) {
                counter(filesCounter, catalog).increment(processed);
            }
            log.debug("Compaction step {} on {} done{}", step, catalog,
                    filesCounter == null ? "" : " (" + processed + " files)");
        } catch (Exception e) {
            counter("failures", catalog, "step", step).increment();
            if (isConflict(e)) {
                log.info("Compaction step {} on {} lost a transaction conflict; retrying next run", step, catalog);
            } else {
                log.error("Compaction step {} on {} failed", step, catalog, e);
            }
        } finally {
            running = null;
            sample.stop(Timer.builder("dazzleduck.otel.compaction.duration")
                    .description("Time per compaction step")
                    .tag("catalog", catalog)
                    .tag("step", step)
                    .register(registry));
        }
    }

    private Counter counter(String name, String catalog, String... extraTags) {
        String description = switch (name) {
            case "files_merged" -> "Data files merged away by compaction";
            case "files_rewritten" -> "Data files rewritten to drop their delete files";
            default -> "Compaction steps that failed, including lost transaction conflicts";
        };
        return Counter.builder("dazzleduck.otel.compaction." + name)
                .description(description)
                .tag("catalog", catalog)
                .tags(extraTags)
                .register(registry);
    }

    private static boolean isConflict(Throwable e) {
        for (Throwable c = e; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().toLowerCase().contains("transaction conflict")) {
                return true;
            }
        }
        return false;
    }

    /** Stops scheduling; waits briefly for a running step, then cancels it. */
    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                Statement statement = running;
                if (statement != null) {
                    try {
                        statement.cancel();
                    } catch (SQLException e) {
                        log.warn("Could not cancel the running compaction step", e);
                    }
                }
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
