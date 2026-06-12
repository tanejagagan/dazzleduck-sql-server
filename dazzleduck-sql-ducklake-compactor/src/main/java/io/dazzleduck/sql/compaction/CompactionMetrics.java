package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionMetrics {

    private static final String DURATION_METRIC = "ducklake.compaction.duration";
    private static final String SMALL_FILES_METRIC = "ducklake.files.small";
    private static final String MEDIUM_FILES_METRIC = "ducklake.files.medium";
    private static final String TOTAL_FILES_METRIC = "ducklake.files.total";

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, AtomicLong> smallFileCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> mediumFileCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> totalFileCounts = new ConcurrentHashMap<>();

    public CompactionMetrics(MeterRegistry registry, List<String> databases) {
        this.registry = registry;
        databases.forEach(this::registerDatabase);
    }

    private void registerDatabase(String database) {
        AtomicLong small = smallFileCounts.computeIfAbsent(database, k -> new AtomicLong(0));
        AtomicLong medium = mediumFileCounts.computeIfAbsent(database, k -> new AtomicLong(0));
        AtomicLong total = totalFileCounts.computeIfAbsent(database, k -> new AtomicLong(0));

        Gauge.builder(SMALL_FILES_METRIC, small, AtomicLong::get)
                .description("Active files smaller than minor_compaction_max_size")
                .tag("database", database)
                .register(registry);

        Gauge.builder(MEDIUM_FILES_METRIC, medium, AtomicLong::get)
                .description("Active files between minor and major compaction max size")
                .tag("database", database)
                .register(registry);

        Gauge.builder(TOTAL_FILES_METRIC, total, AtomicLong::get)
                .description("Total active files")
                .tag("database", database)
                .register(registry);
    }

    public void updateFileCounts(String database, long small, long medium, long total) {
        smallFileCounts.computeIfAbsent(database, k -> new AtomicLong(0)).set(small);
        mediumFileCounts.computeIfAbsent(database, k -> new AtomicLong(0)).set(medium);
        totalFileCounts.computeIfAbsent(database, k -> new AtomicLong(0)).set(total);
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String type, String step, String database) {
        sample.stop(Timer.builder(DURATION_METRIC)
                .description("Duration of compaction step")
                .tag("type", type)
                .tag("step", step)
                .tag("database", database)
                .register(registry));
    }
}
