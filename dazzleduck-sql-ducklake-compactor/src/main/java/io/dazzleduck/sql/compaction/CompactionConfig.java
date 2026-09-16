package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;

import java.time.Duration;
import java.util.List;

public record CompactionConfig(
        List<String> databases,
        Duration minorCompactionFrequency,
        Duration majorCompactionFrequency,
        Duration housekeepingFrequency,
        long minorCompactionMaxSize,
        long minorCompactionMaxFiles,
        long majorCompactionMaxSize,
        long majorCompactionMaxFiles,
        Duration snapshotRetention,
        int healthPort,
        boolean minorCompactionEnabled,
        List<String> minorConnectionSettings,
        boolean majorCompactionEnabled,
        List<String> majorConnectionSettings
) {
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
        long minorMaxSize = c.getBytes("minor_compaction_max_size");
        long majorMaxSize = c.getBytes("major_compaction_max_size");
        boolean minorEnabled = c.getBoolean("minor_compaction.enabled");
        boolean majorEnabled = c.getBoolean("major_compaction.enabled");

        // Minor and major run concurrently with no lock, safe only because they're fenced to
        // disjoint file-size ranges: minor handles [0, minorMaxSize), major handles
        // [minorMaxSize, majorMaxSize) via min_file_size/max_file_size. If both are enabled and
        // this ordering doesn't hold, major's range is empty or inverted — refuse to start rather
        // than run silently on a config that can never do anything (or, worse, overlap).
        if (minorEnabled && majorEnabled && majorMaxSize <= minorMaxSize) {
            throw new IllegalArgumentException(
                    "major_compaction_max_size (%d) must be greater than minor_compaction_max_size (%d) when both minor and major compaction are enabled"
                            .formatted(majorMaxSize, minorMaxSize));
        }

        return new CompactionConfig(
                c.getStringList("databases"),
                c.getDuration("minor_compaction_frequency"),
                c.getDuration("major_compaction_frequency"),
                c.getDuration("housekeeping_frequency"),
                minorMaxSize,
                c.getLong("minor_compaction_max_files"),
                majorMaxSize,
                c.getLong("major_compaction_max_files"),
                c.getDuration("snapshot_retention"),
                c.getInt("health_port"),
                minorEnabled,
                c.getStringList("minor_compaction.connection_settings"),
                majorEnabled,
                c.getStringList("major_compaction.connection_settings")
        );
    }
}
