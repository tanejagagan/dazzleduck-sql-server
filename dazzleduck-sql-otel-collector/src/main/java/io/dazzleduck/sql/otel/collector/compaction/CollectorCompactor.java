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
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    static final String MINOR = "minor";
    static final String MAJOR = "major";
    static final String ORPHAN_CLEANUP = "orphan_cleanup";

    /** Outcome of a step, and of a job run as its worst step. */
    public enum Outcome { OK, CONFLICT, FAILED }

    /**
     * Last run of one job on one catalog, for the {@code /stats} page. Times are null until the job
     * has run; {@code nextRun} is when the scheduler will start it next.
     */
    public record JobStatus(String catalog, String job, Instant lastStart, long lastDurationMs, Outcome lastOutcome,
                            String lastError, long lastFilesMerged, long lastFilesRewritten, long totalFilesMerged,
                            long totalFilesRewritten, long runs, long failedRuns, Instant nextRun) {}

    /** Everything the {@code /stats} page shows about compaction. */
    public record Status(boolean enabled, List<JobStatus> jobs, Map<String, Long> snapshotCounts) {}

    private final CompactionSettings settings;
    private final MeterRegistry registry;
    private final ScheduledExecutorService scheduler;
    // The statement currently executing, so close() can cancel a long merge.
    private volatile Statement running;
    // catalog|job -> last run, in catalog then job order.
    private final Map<String, JobStatus> statuses = new ConcurrentHashMap<>();
    // catalog -> snapshot count, refreshed on the compaction thread after each job run, so status()
    // never touches the catalog database.
    private final Map<String, Long> snapshotCounts = new ConcurrentHashMap<>();

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
        schedule(MINOR, this::runMinor, settings.minorFrequency());
        schedule(MAJOR, this::runMajor, settings.majorFrequency());
        if (settings.orphanCleanupEnabled()) {
            schedule(ORPHAN_CLEANUP, this::runOrphanCleanup, settings.orphanFrequency());
        }
        log.info("Compaction enabled for {}: minor every {} (files < {} bytes), major every {} (retention {}), "
                        + "orphan cleanup {}",
                settings.catalogs(), settings.minorFrequency(), settings.minorMaxFileSize(),
                settings.majorFrequency(), settings.snapshotRetention(),
                settings.orphanCleanupEnabled()
                        ? "every " + settings.orphanFrequency() + " (older than " + settings.orphanOlderThan() + ")"
                        : "off");
    }

    private void schedule(String job, Runnable run, Duration every) {
        Instant first = Instant.now().plus(every);
        for (String catalog : settings.catalogs()) {
            statuses.put(key(catalog, job), new JobStatus(catalog, job, null, 0, null, null, 0, 0, 0, 0, 0, 0, first));
        }
        long ms = every.toMillis();
        // Fixed delay: a slow run pushes the next one back instead of queueing runs behind it.
        scheduler.scheduleWithFixedDelay(run, ms, ms, TimeUnit.MILLISECONDS);
    }

    /** Merges files smaller than {@code minor.max_file_size}. */
    void runMinor() {
        for (String catalog : settings.catalogs()) {
            var run = new JobRun(catalog, MINOR, settings.minorFrequency());
            run.add(step(catalog, "minor_merge", "CALL ducklake_merge_adjacent_files('%s', max_file_size => %d)"
                    .formatted(catalog, settings.minorMaxFileSize()), "files_merged"));
            run.finish();
        }
    }

    /** Flushes inlined data, expires snapshots, merges all sizes, rewrites deletes, deletes retired files. */
    void runMajor() {
        long retentionSeconds = settings.snapshotRetention().toSeconds();
        for (String catalog : settings.catalogs()) {
            var run = new JobRun(catalog, MAJOR, settings.majorFrequency());
            run.add(step(catalog, "flush_inlined", "CALL ducklake_flush_inlined_data('%s')".formatted(catalog), null));
            run.add(step(catalog, "expire_snapshots", "CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, retentionSeconds), null));
            run.add(step(catalog, "major_merge", "CALL ducklake_merge_adjacent_files('%s')".formatted(catalog), "files_merged"));
            run.add(step(catalog, "rewrite_deletes", settings.rewriteDeleteThreshold() == null
                    ? "CALL ducklake_rewrite_data_files('%s')".formatted(catalog)
                    : "CALL ducklake_rewrite_data_files('%s', delete_threshold => %s)"
                            .formatted(catalog, settings.rewriteDeleteThreshold()), "files_rewritten"));
            run.add(step(catalog, "cleanup_old_files", "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, retentionSeconds), null));
            run.finish();
        }
    }

    /** Deletes files under the catalog's data path that it does not reference and that are old enough. */
    void runOrphanCleanup() {
        long ageSeconds = settings.orphanOlderThan().toSeconds();
        for (String catalog : settings.catalogs()) {
            var run = new JobRun(catalog, ORPHAN_CLEANUP, settings.orphanFrequency());
            run.add(step(catalog, "delete_orphaned_files", "CALL ducklake_delete_orphaned_files('%s', older_than => now() - INTERVAL '%d seconds')"
                    .formatted(catalog, ageSeconds), null));
            run.finish();
        }
    }

    /** What one step did; {@code files} counts toward {@code filesCounter} when set. */
    private record StepResult(String step, String filesCounter, long files, Outcome outcome, String error) {}

    /**
     * Runs one step and records its duration. Never throws: a scheduled task that throws is never
     * run again. When {@code filesCounter} is set, the step's {@code files_processed} total is added
     * to that counter.
     */
    private StepResult step(String catalog, String step, String sql, String filesCounter) {
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
            return new StepResult(step, filesCounter, processed, Outcome.OK, null);
        } catch (Exception e) {
            counter("failures", catalog, "step", step).increment();
            if (isConflict(e)) {
                log.info("Compaction step {} on {} lost a transaction conflict; retrying next run", step, catalog);
                return new StepResult(step, filesCounter, 0, Outcome.CONFLICT, step + ": " + rootMessage(e));
            }
            log.error("Compaction step {} on {} failed", step, catalog, e);
            return new StepResult(step, filesCounter, 0, Outcome.FAILED, step + ": " + rootMessage(e));
        } finally {
            running = null;
            sample.stop(Timer.builder("dazzleduck.otel.compaction.duration")
                    .description("Time per compaction step")
                    .tag("catalog", catalog)
                    .tag("step", step)
                    .register(registry));
        }
    }

    /** Folds a job's steps into its {@link JobStatus}. */
    private final class JobRun {
        private final String catalog;
        private final String job;
        private final Duration every;
        private final Instant start = Instant.now();
        private final long startNanos = System.nanoTime();
        private Outcome outcome = Outcome.OK;
        private String error;
        private long merged;
        private long rewritten;

        JobRun(String catalog, String job, Duration every) {
            this.catalog = catalog;
            this.job = job;
            this.every = every;
        }

        void add(StepResult result) {
            if ("files_merged".equals(result.filesCounter())) merged += result.files();
            if ("files_rewritten".equals(result.filesCounter())) rewritten += result.files();
            if (result.outcome().ordinal() > outcome.ordinal()) outcome = result.outcome();
            if (error == null && result.error() != null) error = result.error();
        }

        void finish() {
            long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
            statuses.compute(key(catalog, job), (k, prev) -> new JobStatus(catalog, job, start, durationMs, outcome, error,
                    merged, rewritten,
                    (prev == null ? 0 : prev.totalFilesMerged()) + merged,
                    (prev == null ? 0 : prev.totalFilesRewritten()) + rewritten,
                    (prev == null ? 0 : prev.runs()) + 1,
                    (prev == null ? 0 : prev.failedRuns()) + (outcome == Outcome.OK ? 0 : 1),
                    Instant.now().plus(every)));
            snapshotCounts.put(catalog, snapshotCount(catalog));
        }
    }

    /**
     * Current status for the {@code /stats} page. Memory only: the health server answers
     * {@code /health} probes on the same thread, so a page view must never wait on the catalog
     * database. Snapshot counts are as of each catalog's last job run (absent before its first run,
     * -1 when it could not be read).
     */
    public Status status() {
        List<JobStatus> jobs = new ArrayList<>();
        for (String catalog : settings.catalogs()) {
            for (String job : List.of(MINOR, MAJOR, ORPHAN_CLEANUP)) {
                JobStatus status = statuses.get(key(catalog, job));
                if (status != null) jobs.add(status);
            }
        }
        Map<String, Long> snapshots = new LinkedHashMap<>();
        for (String catalog : settings.catalogs()) {
            Long count = snapshotCounts.get(catalog);
            if (count != null) snapshots.put(catalog, count);
        }
        return new Status(settings.enabled(), jobs, snapshots);
    }

    private static long snapshotCount(String catalog) {
        try {
            Long count = ConnectionPool.collectFirst(
                    "SELECT count(*) FROM \"__ducklake_metadata_%s\".ducklake_snapshot".formatted(catalog), Long.class);
            return count == null ? -1 : count;
        } catch (Exception e) {
            return -1;
        }
    }

    private static String key(String catalog, String job) {
        return catalog + "|" + job;
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

    private static String rootMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        String message = root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
        return message.length() > 300 ? message.substring(0, 300) + "…" : message;
    }

    /**
     * Stops scheduling; waits briefly for a running step, then cancels it and waits for the cancelled
     * step to stop, so no step is still running when the caller goes on to close queues.
     */
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
                if (!scheduler.awaitTermination(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    log.warn("A compaction step was still running {} after it was cancelled", CLOSE_TIMEOUT);
                }
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
