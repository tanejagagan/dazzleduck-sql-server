package io.dazzleduck.sql.compaction;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.TableConfigProvider;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.common.StartupScriptProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Config rawConfig = CompactionConfig.rawConfig(args);

        // The startup script is what ATTACHes the catalog, so it must run before a config provider
        // that reads a table in it. Ordering is the whole trick: file config -> attach -> overlay.
        // Run once here (via the shared commons ConnectionPool, since TableConfigProvider's own table
        // read depends on it) purely so the config-provider table below can be read; the script text
        // itself is captured and handed to every raw compaction/housekeeping connection too, each of
        // which independently re-runs it on its own real DuckDB instance (see RawConnections).
        String startupScript = readStartupScript(rawConfig);
        if (startupScript != null) {
            ConnectionPool.executeOnSingleton(startupScript);
        }

        CompactionConfig config = CompactionConfig.from(withOverrides(rawConfig));

        CompactionMetrics metrics = CompactionMetrics.create(rawConfig.getConfig("metrics"));
        List<String> tierNames = config.tiers().stream().map(CompactionTier::name).toList();
        CompactionState state = new CompactionState(metrics.registry(), config.databases(), tierNames);
        TierCompactor tierCompactor = new DuckDbTierCompactor(startupScript, state);
        Housekeeper housekeeper = new DuckLakeHousekeeper(
                startupScript, config.snapshotRetention(), config.housekeepingConnectionSettings(),
                config.rewriteDeletesEnabled(), config.rewriteDeleteThreshold(), state);
        CompactionRunLog runLog = new CompactionRunLog(config.runHistorySize());
        CompactionService service = new CompactionService(config, startupScript, tierCompactor, housekeeper, state, runLog);
        HealthServer healthServer = new HealthServer(config.healthPort(), service::getStats, runLog);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            service.close();
            healthServer.close();
            metrics.close();
        }, "shutdown-hook"));

        healthServer.start();
        service.start();

        Thread.currentThread().join();
    }

    /**
     * Overlays a {@link ConfigProvider}'s values on the file-based config, or returns it unchanged
     * when no provider is configured.
     *
     * <p>A configured provider that cannot be read is FATAL. The alternative — start on the bundled
     * defaults — is worse here than it looks: an operator who has moved compaction settings into a
     * table will not be watching the file, so a silent fallback runs the lake on values nobody has
     * reviewed in months, and the symptom (files growing, snapshots expiring early) is invisible
     * until something downstream stalls. Refusing to start is loud, and the previous pod keeps
     * running under an orchestrator.
     */
    private static Config withOverrides(Config rawConfig) throws Exception {
        TableConfigProvider provider = TableConfigProvider.load(rawConfig);
        if (provider == null) {
            return rawConfig;
        }
        Config overrides = provider.overrides();
        logger.info("Applied {} config override(s) from the configured provider",
                overrides.entrySet().size());
        return overrides.withFallback(rawConfig);
    }

    /** Returns the configured startup script text, or {@code null} if none is configured. */
    private static String readStartupScript(Config config) throws Exception {
        StartupScriptProvider provider = StartupScriptProvider.load(config);
        String script = provider.getStartupScript();
        return (script != null && !script.isBlank()) ? script : null;
    }
}
