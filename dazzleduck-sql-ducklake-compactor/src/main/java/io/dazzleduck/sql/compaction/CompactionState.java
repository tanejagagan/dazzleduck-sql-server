package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionState {

    private static final String DURATION_METRIC    = "ducklake.compaction.duration";
    private static final String MINOR_COUNT_METRIC = "ducklake.compaction.minor";
    private static final String MAJOR_COUNT_METRIC = "ducklake.compaction.major";
    private static final String FILES_COMPACTED_METRIC = "ducklake.files.compacted";
    private static final String SMALL_FILES_METRIC  = "ducklake.files.small";
    private static final String MEDIUM_FILES_METRIC = "ducklake.files.medium";
    private static final String TOTAL_FILES_METRIC  = "ducklake.files.total";

    private final MeterRegistry registry;
    private final Instant serviceStart = Instant.now();

    // Per-database counters (also back Micrometer FunctionCounters)
    private final ConcurrentHashMap<String, AtomicLong> minorCounts    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> majorCounts    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> filesCompacted = new ConcurrentHashMap<>();

    // Per-database gauges
    private final ConcurrentHashMap<String, AtomicLong> smallFiles  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> mediumFiles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> totalFiles  = new ConcurrentHashMap<>();

    // Per-database execution times (health endpoint only)
    private final ConcurrentHashMap<String, AtomicReference<Instant>> lastExecutionTimes = new ConcurrentHashMap<>();

    public CompactionState(MeterRegistry registry, List<String> databases) {
        this.registry = registry;
        databases.forEach(this::registerDatabase);
    }

    private void registerDatabase(String db) {
        AtomicLong minor  = minorCounts.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong major  = majorCounts.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong files  = filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong small  = smallFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong medium = mediumFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        AtomicLong total  = totalFiles.computeIfAbsent(db, k -> new AtomicLong(0));
        lastExecutionTimes.computeIfAbsent(db, k -> new AtomicReference<>());

        FunctionCounter.builder(MINOR_COUNT_METRIC, minor, AtomicLong::doubleValue)
                .description("Total minor compaction cycles")
                .tag("database", db)
                .register(registry);

        FunctionCounter.builder(MAJOR_COUNT_METRIC, major, AtomicLong::doubleValue)
                .description("Total major compaction cycles")
                .tag("database", db)
                .register(registry);

        FunctionCounter.builder(FILES_COMPACTED_METRIC, files, AtomicLong::doubleValue)
                .description("Total Parquet files merged by compaction")
                .tag("database", db)
                .register(registry);

        Gauge.builder(SMALL_FILES_METRIC, small, AtomicLong::get)
                .description("Active files smaller than minor_compaction_max_size")
                .tag("database", db)
                .register(registry);

        Gauge.builder(MEDIUM_FILES_METRIC, medium, AtomicLong::get)
                .description("Active files between minor and major compaction max size")
                .tag("database", db)
                .register(registry);

        Gauge.builder(TOTAL_FILES_METRIC, total, AtomicLong::get)
                .description("Total active files")
                .tag("database", db)
                .register(registry);
    }

    // ── Update methods ────────────────────────────────────────────────────────

    public void incrementMinor(String db) {
        minorCounts.computeIfAbsent(db, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void incrementMajor(String db) {
        majorCounts.computeIfAbsent(db, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void addFilesCompacted(String db, long delta) {
        if (delta > 0) filesCompacted.computeIfAbsent(db, k -> new AtomicLong(0)).addAndGet(delta);
    }

    public void updateFileCounts(String db, long small, long medium, long total) {
        smallFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(small);
        mediumFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(medium);
        totalFiles.computeIfAbsent(db, k -> new AtomicLong(0)).set(total);
    }

    public void recordLastExecution(String db) {
        lastExecutionTimes.computeIfAbsent(db, k -> new AtomicReference<>()).set(Instant.now());
    }

    // ── Timer helpers ─────────────────────────────────────────────────────────

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String type, String step, String db) {
        sample.stop(Timer.builder(DURATION_METRIC)
                .description("Duration of compaction step")
                .tag("type", type)
                .tag("step", step)
                .tag("database", db)
                .register(registry));
    }

    // ── Snapshot for health endpoint ──────────────────────────────────────────

    public CompactionStats getSnapshot(List<String> databases) {
        Map<String, CompactionStats.DatabaseStats> dbStats = new HashMap<>();
        for (String db : databases) {
            AtomicReference<Instant> lastRef = lastExecutionTimes.get(db);
            Instant last = lastRef != null ? lastRef.get() : null;
            dbStats.put(db, new CompactionStats.DatabaseStats(
                    minorCounts.getOrDefault(db, new AtomicLong(0)).get(),
                    majorCounts.getOrDefault(db, new AtomicLong(0)).get(),
                    filesCompacted.getOrDefault(db, new AtomicLong(0)).get(),
                    last,
                    null, // nextExecutionTime injected by CompactionService
                    smallFiles.getOrDefault(db, new AtomicLong(0)).get(),
                    mediumFiles.getOrDefault(db, new AtomicLong(0)).get(),
                    totalFiles.getOrDefault(db, new AtomicLong(0)).get()));
        }
        return new CompactionStats(serviceStart, dbStats);
    }

    public Instant getLastExecutionTime(String db) {
        AtomicReference<Instant> ref = lastExecutionTimes.get(db);
        return ref != null ? ref.get() : null;
    }
}
