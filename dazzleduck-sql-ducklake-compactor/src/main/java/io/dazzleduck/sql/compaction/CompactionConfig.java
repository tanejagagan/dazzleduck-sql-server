package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.util.CommandLineConfigUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record CompactionConfig(
        List<String> databases,
        Duration minorCompactionFrequency,
        Duration majorCompactionFrequency,
        Duration housekeepingFrequency,
        long minorCompactionMaxSize,
        long majorCompactionMaxSize,
        Duration snapshotRetention,
        int healthPort,
        Duration idleInTransactionTimeout,
        Duration idleInTransactionTimeoutMax,
        boolean idleInTransactionTimeoutAdaptive,
        Map<String, PostgresMetadataConfig> postgresMetadata
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
        return new CompactionConfig(
                c.getStringList("databases"),
                c.getDuration("minor_compaction_frequency"),
                c.getDuration("major_compaction_frequency"),
                c.getDuration("housekeeping_frequency"),
                c.getBytes("minor_compaction_max_size"),
                c.getBytes("major_compaction_max_size"),
                c.getDuration("snapshot_retention"),
                c.getInt("health_port"),
                c.getDuration("idle_in_transaction_timeout"),
                c.getDuration("idle_in_transaction_timeout_max"),
                c.getBoolean("idle_in_transaction_timeout_adaptive"),
                parsePostgresMetadata(c)
        );
    }

    /**
     * List-of-objects, not a keyed map, to match this repo's existing convention for per-alias
     * override data (e.g. {@code ingestion_queue_table_mapping}).
     */
    private static Map<String, PostgresMetadataConfig> parsePostgresMetadata(Config c) {
        if (!c.hasPath("postgres_metadata")) {
            return Map.of();
        }
        return c.getConfigList("postgres_metadata").stream()
                .map(entry -> new PostgresMetadataConfig(
                        entry.getString("database"),
                        entry.getString("connection_string"),
                        entry.getString("attach_options")))
                .collect(Collectors.toMap(PostgresMetadataConfig::database, p -> p));
    }
}
