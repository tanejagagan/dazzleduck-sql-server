package io.dazzleduck.sql.commons.ingestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read/write access to the SQLite-backed ingestion queue registry.
 *
 * <p>Schema:
 * <pre>
 *   ingestion_queues  — one row per queue; deleted row = tombstone
 *   schema_version    — single row (id=1); owned by the writer to track remote sync progress.
 *                       The writer bumps version atomically with queue changes so it can
 *                       resume from the correct position after a restart.
 * </pre>
 *
 * <p>Change detection for {@link DynamicIngestionHandler} uses {@code PRAGMA data_version},
 * which SQLite increments automatically on every committed write — no writer cooperation needed.
 * {@code schema_version} is separate and exists solely for the writer's remote-sync bookkeeping.
 *
 * <p>The writer opens the database in read-write mode and initialises the schema on first use.
 * A second, read-only connection (used by {@link DynamicIngestionHandler}) is safe to open
 * concurrently because WAL mode is enabled at init time.
 */
public class SqliteQueueRepository implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SqliteQueueRepository.class);

    static final String[] DDL_STATEMENTS = {
        "PRAGMA journal_mode=WAL",
        """
        CREATE TABLE IF NOT EXISTS ingestion_queues (
            ingestion_queue  TEXT PRIMARY KEY,
            output_path      TEXT NOT NULL,
            catalog          TEXT NOT NULL,
            schema_name      TEXT NOT NULL,
            table_name       TEXT NOT NULL,
            transformation   TEXT,
            view_name        TEXT,
            input_table      TEXT,
            partition_by     TEXT,
            min_bucket_size  INTEGER,
            max_delay_ms     INTEGER
        )""",
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            id      INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL DEFAULT 0
        )""",
        "INSERT OR IGNORE INTO schema_version (id, version) VALUES (1, 0)"
    };

    private final String jdbcUrl;
    private Connection connection;

    public SqliteQueueRepository(String dbPath) {
        this.jdbcUrl = "jdbc:sqlite:" + dbPath;
    }

    /** Opens the connection and runs DDL (idempotent — all statements use IF NOT EXISTS). */
    public synchronized void init() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl);
        try (Statement st = connection.createStatement()) {
            for (String stmt : DDL_STATEMENTS) {
                st.execute(stmt);
            }
        }
        logger.debug("SQLite ingestion repository initialised at {}", jdbcUrl);
    }

    /**
     * Opens a separate read-only connection to the same database for use by
     * {@link DynamicIngestionHandler}. The caller owns the returned connection and must
     * close it.
     *
     * <p>WAL mode allows one read-only reader and one read-write writer to coexist without
     * blocking each other.
     */
    public Connection openReadOnlyConnection() throws SQLException {
        Connection c = DriverManager.getConnection(jdbcUrl + "?open_mode=1");
        // open_mode=1 is SQLite READONLY; equivalent to SQLITE_OPEN_READONLY.
        return c;
    }

    // -----------------------------------------------------------------------
    // Change detection — used by DynamicIngestionHandler
    // -----------------------------------------------------------------------

    /**
     * Returns the current {@code PRAGMA data_version} for the supplied connection.
     * SQLite increments this automatically on every committed write, regardless of which
     * table was modified or which connection did the write. Returns -1 on error.
     */
    public static long readDataVersion(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA data_version")) {
            return rs.next() ? rs.getLong(1) : -1L;
        } catch (SQLException e) {
            logger.debug("data_version not readable ({}), returning -1", e.getMessage());
            return -1L;
        }
    }

    // -----------------------------------------------------------------------
    // Writer helper — remote-sync bookkeeping via schema_version
    // -----------------------------------------------------------------------

    /**
     * Reads the writer's remote-sync version from {@code schema_version}.
     * Returns -1 if the table is not yet initialised.
     */
    public static long readSchemaVersion(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT version FROM schema_version WHERE id = 1")) {
            return rs.next() ? rs.getLong(1) : -1L;
        } catch (SQLException e) {
            logger.debug("schema_version not readable ({}), returning -1", e.getMessage());
            return -1L;
        }
    }

    /**
     * Loads all rows from {@code ingestion_queues} using the supplied connection and returns
     * them as a queue-id-keyed map. The connection is not closed by this method.
     */
    public static Map<String, QueueIdToTableMapping> loadAll(Connection conn) throws SQLException {
        String sql = "SELECT ingestion_queue, output_path, catalog, schema_name, table_name, " +
                     "transformation, view_name, input_table, partition_by, " +
                     "min_bucket_size, max_delay_ms FROM ingestion_queues";
        Map<String, QueueIdToTableMapping> result = new LinkedHashMap<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String queueId    = rs.getString("ingestion_queue");
                String outputPath = rs.getString("output_path");
                String catalog    = rs.getString("catalog");
                String schema     = rs.getString("schema_name");
                String table      = rs.getString("table_name");
                String transform  = rs.getString("transformation");
                String view       = rs.getString("view_name");
                String inputTable = rs.getString("input_table");

                // catalog/schema/table may be null for write-only queues
                QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                        queueId, outputPath, catalog, schema, table,
                        Map.of(), transform, view, inputTable);
                result.put(queueId, mapping);
            }
        }
        return result;
    }

    @Override
    public synchronized void close() {
        if (connection != null) {
            try { connection.close(); } catch (SQLException e) {
                logger.warn("Failed to close SQLite repository connection", e);
            }
            connection = null;
        }
    }
}
