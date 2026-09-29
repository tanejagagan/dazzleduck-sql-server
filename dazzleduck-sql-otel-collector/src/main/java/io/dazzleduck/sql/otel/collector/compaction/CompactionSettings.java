package io.dazzleduck.sql.otel.collector.compaction;

import com.typesafe.config.Config;

import java.time.Duration;
import java.util.List;

/**
 * The {@code otel_collector.compaction} block: DuckLake maintenance run inside the collector, on
 * the collector's own DuckDB instance (see {@link CollectorCompactor}).
 *
 * @param enabled                global switch; nothing is scheduled when false
 * @param catalogs               attached DuckLake catalog names to maintain
 * @param minorFrequency         delay between minor runs (merge small files)
 * @param minorMaxFileSize       only files smaller than this are merged by a minor run
 * @param majorFrequency         delay between major runs (flush, expire, merge, rewrite, clean up)
 * @param snapshotRetention      snapshots older than this are expired, and files retired longer
 *                               ago than this are deleted
 * @param rewriteDeleteThreshold fraction (0-1) of a file's rows that must be deleted before a major
 *                               run rewrites it, or null for the catalog's rewrite_delete_threshold
 * @param orphanCleanupEnabled   whether to delete files the catalog does not reference
 * @param orphanFrequency        delay between orphan cleanups
 * @param orphanOlderThan        only unreferenced files older than this are deleted
 */
public record CompactionSettings(
        boolean enabled,
        List<String> catalogs,
        Duration minorFrequency,
        long minorMaxFileSize,
        Duration majorFrequency,
        Duration snapshotRetention,
        Double rewriteDeleteThreshold,
        boolean orphanCleanupEnabled,
        Duration orphanFrequency,
        Duration orphanOlderThan) {

    /**
     * Lower bound for {@code orphan_cleanup.older_than}. The collector writes a batch's Parquet file
     * before registering it, so until then the file is an orphan; deleting it loses the batch.
     */
    public static final Duration MIN_ORPHAN_AGE = Duration.ofHours(1);

    public static CompactionSettings disabled() {
        // 8MB as HOCON reads it (decimal), matching reference.conf.
        return new CompactionSettings(false, List.of(), Duration.ofMinutes(1), 8_000_000L,
                Duration.ofHours(1), Duration.ofMinutes(15), null, false, Duration.ofDays(1), Duration.ofDays(2));
    }

    /**
     * Parses the block strictly: a wrong type throws Typesafe Config's exception naming the key, and
     * an invalid value throws {@link IllegalArgumentException}, so a bad block fails startup.
     */
    public static CompactionSettings from(Config c) {
        var defaults = disabled();
        var settings = new CompactionSettings(
                c.hasPath("enabled") && c.getBoolean("enabled"),
                c.hasPath("catalogs") ? List.copyOf(c.getStringList("catalogs")) : List.of(),
                c.hasPath("minor.frequency") ? c.getDuration("minor.frequency") : defaults.minorFrequency(),
                c.hasPath("minor.max_file_size") ? c.getBytes("minor.max_file_size") : defaults.minorMaxFileSize(),
                c.hasPath("major.frequency") ? c.getDuration("major.frequency") : defaults.majorFrequency(),
                c.hasPath("major.snapshot_retention")
                        ? c.getDuration("major.snapshot_retention") : defaults.snapshotRetention(),
                c.hasPath("major.rewrite_delete_threshold") ? c.getDouble("major.rewrite_delete_threshold") : null,
                c.hasPath("orphan_cleanup.enabled") && c.getBoolean("orphan_cleanup.enabled"),
                c.hasPath("orphan_cleanup.frequency")
                        ? c.getDuration("orphan_cleanup.frequency") : defaults.orphanFrequency(),
                c.hasPath("orphan_cleanup.older_than")
                        ? c.getDuration("orphan_cleanup.older_than") : defaults.orphanOlderThan());
        settings.validate();
        return settings;
    }

    private void validate() {
        requirePositive("minor.frequency", minorFrequency);
        requirePositive("major.frequency", majorFrequency);
        requirePositive("orphan_cleanup.frequency", orphanFrequency);
        if (minorMaxFileSize <= 0) {
            throw new IllegalArgumentException("compaction.minor.max_file_size must be positive, got " + minorMaxFileSize);
        }
        if (snapshotRetention.isNegative()) {
            throw new IllegalArgumentException("compaction.major.snapshot_retention must not be negative");
        }
        if (rewriteDeleteThreshold != null && (rewriteDeleteThreshold < 0 || rewriteDeleteThreshold > 1)) {
            throw new IllegalArgumentException(
                    "compaction.major.rewrite_delete_threshold must be between 0 and 1, got " + rewriteDeleteThreshold);
        }
        if (orphanOlderThan.compareTo(MIN_ORPHAN_AGE) < 0) {
            throw new IllegalArgumentException(("compaction.orphan_cleanup.older_than must be at least %s, got %s:"
                    + " a batch's file is unreferenced until it is registered, so a shorter age can delete"
                    + " live ingestion data").formatted(MIN_ORPHAN_AGE, orphanOlderThan));
        }
        if (enabled && catalogs.isEmpty()) {
            throw new IllegalArgumentException("compaction.enabled is true but compaction.catalogs is empty");
        }
        for (String catalog : catalogs) {
            if (!catalog.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalArgumentException("compaction.catalogs entry is not a plain identifier: '" + catalog + "'");
            }
        }
    }

    private static void requirePositive(String key, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("compaction." + key + " must be positive, got " + value);
        }
    }
}
