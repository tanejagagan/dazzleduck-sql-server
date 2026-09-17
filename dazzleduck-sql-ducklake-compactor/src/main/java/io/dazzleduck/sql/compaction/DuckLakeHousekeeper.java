package io.dazzleduck.sql.compaction;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DuckLakeHousekeeper implements Housekeeper {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeHousekeeper.class);

    private final String startupScript;
    private final Duration snapshotRetention;
    private final List<String> connectionSettings;
    private final CompactionState metrics;

    // One real DuckDB instance per database — see RawConnections. Keyed by database, not shared:
    // CompactionService sizes its housekeeping thread pool so different databases' housekeeping runs
    // concurrently, and a plain JDBC Connection is not safe for concurrent use by more than one
    // thread. Opened lazily on first use and reused for that database's whole lifetime.
    private final ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<>();

    public DuckLakeHousekeeper(String startupScript, Duration snapshotRetention, List<String> connectionSettings, CompactionState metrics) {
        this.startupScript = startupScript;
        this.snapshotRetention = snapshotRetention;
        this.connectionSettings = connectionSettings;
        this.metrics = metrics;
    }

    @Override
    public void housekeep(String database) throws Exception {
        long retentionSeconds = snapshotRetention.toSeconds();
        Connection connection = connectionFor(database);

        // Run steps independently so a failure in expire does not silently skip cleanup
        try {
            time("housekeeping", "expire", database, () -> execute(connection,
                    "CALL ducklake_expire_snapshots('%s', older_than => now() - INTERVAL '%d seconds')"
                            .formatted(database, retentionSeconds)));
        } catch (Exception e) {
            logger.error("Snapshot expiry failed for {}, cleanup will still run", database, e);
        }
        time("housekeeping", "cleanup", database, () -> execute(connection,
                "CALL ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%d seconds')"
                        .formatted(database, retentionSeconds)));
    }

    private Connection connectionFor(String database) throws SQLException {
        Connection existing = connections.get(database);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = connections.get(database);
            if (existing != null) {
                return existing;
            }
            Connection opened = RawConnections.open(startupScript, connectionSettings);
            connections.put(database, opened);
            return opened;
        }
    }

    private static void execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void time(String type, String step, String database, Runnable action) {
        Timer.Sample sample = metrics.startTimer();
        try {
            action.run();
        } finally {
            metrics.stopTimer(sample, type, step, database);
        }
        logger.debug("{} step '{}' completed for {}", type, step, database);
    }

    @Override
    public void close() {
        connections.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("Failed to close a housekeeping connection", e);
            }
        });
    }
}
