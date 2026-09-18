package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record CompactionConfig(
        List<String> databases,
        List<CompactionTier> tiers,
        Duration housekeepingFrequency,
        Duration snapshotRetention,
        List<String> housekeepingConnectionSettings,
        int healthPort,
        Duration fileCountRefreshFrequency,
        int runHistorySize
) {
    /** Slow cadence for the whole-catalog file-count gauge refresh (off the compaction control path). */
    private static final Duration DEFAULT_FILE_COUNT_REFRESH = Duration.ofSeconds(30);
    private static final String CONFIG_PATH = "dazzleduck_sql_compaction";

    public static Config rawConfig(String[] args) throws Exception {
        Config overrides = CommandLineConfigUtil.loadCommandLineConfig(args).config();
        return overrides
                .withFallback(ConfigFactory.load("application"))
                .withFallback(ConfigFactory.systemProperties())
                .resolve()
                .getConfig(CONFIG_PATH);
    }

    public static CompactionConfig load(String[] args) throws Exception {
        return from(rawConfig(args));
    }

    static CompactionConfig from(Config c) {
        List<CompactionTier> tiers = parseTiers(c);
        validateTiers(tiers);

        return new CompactionConfig(
                c.getStringList("databases"),
                tiers,
                c.getDuration("housekeeping_frequency"),
                c.getDuration("snapshot_retention"),
                c.getStringList("housekeeping_connection_settings"),
                c.getInt("health_port"),
                c.hasPath("file_count_refresh_frequency")
                        ? c.getDuration("file_count_refresh_frequency") : DEFAULT_FILE_COUNT_REFRESH,
                c.hasPath("run_history_size")
                        ? c.getInt("run_history_size") : CompactionRunLog.DEFAULT_CAPACITY
        );
    }

    private static List<CompactionTier> parseTiers(Config c) {
        return c.getConfigList("compaction_tiers").stream()
                .map(t -> new CompactionTier(
                        t.getString("name"),
                        t.getBoolean("enabled"),
                        t.getDuration("frequency"),
                        t.getBytes("min_file_size"),
                        t.getBytes("max_file_size"),
                        t.getLong("max_compacted_files"),
                        t.getStringList("connection_settings")))
                .toList();
    }

    /**
     * Tiers run concurrently with no lock between them, safe only because their file-size ranges
     * never overlap. Refuses to start rather than run silently on a config that could race — the
     * same "loud failure over silent misconfiguration" philosophy the rest of this config uses.
     */
    private static void validateTiers(List<CompactionTier> tiers) {
        Set<String> names = new HashSet<>();
        for (CompactionTier tier : tiers) {
            if (!names.add(tier.name())) {
                throw new IllegalArgumentException("Duplicate compaction tier name: " + tier.name());
            }
            if (tier.enabled() && tier.maxFileSize() <= tier.minFileSize()) {
                throw new IllegalArgumentException(
                        "Compaction tier '%s': max_file_size (%d) must be greater than min_file_size (%d)"
                                .formatted(tier.name(), tier.maxFileSize(), tier.minFileSize()));
            }
        }

        List<CompactionTier> enabledSorted = tiers.stream()
                .filter(CompactionTier::enabled)
                .sorted(Comparator.comparingLong(CompactionTier::minFileSize))
                .toList();
        for (int i = 1; i < enabledSorted.size(); i++) {
            CompactionTier previous = enabledSorted.get(i - 1);
            CompactionTier current = enabledSorted.get(i);
            if (current.minFileSize() < previous.maxFileSize()) {
                throw new IllegalArgumentException(
                        "Compaction tiers '%s' [%d, %d) and '%s' [%d, %d) overlap"
                                .formatted(previous.name(), previous.minFileSize(), previous.maxFileSize(),
                                        current.name(), current.minFileSize(), current.maxFileSize()));
            }
        }
    }
}
