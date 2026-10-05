package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read/write access to the SQLite-backed ingestion queue registry via DuckDB's SQLite extension.
 *
 * <p>Schema:
 * <pre>
 *   ingestion_queues  — one row per queue; deleted row = tombstone
 *   schema_version    — single row (id=1); owned by the writer to track remote sync progress.
 * </pre>
 *
 * <p>Change detection for {@link DynamicIngestionHandler} polls {@link #readSchemaVersion(Connection)}
 * via its persistent connection. {@link #readDataVersion(String)} (file mtime) is a separate,
 * lighter-weight check available for callers that don't hold a persistent connection open — but
 * it is unsuitable for a long-lived poller on FUSE-backed mounts (see {@link DynamicIngestionHandler}'s
 * class javadoc), so {@link DynamicIngestionHandler} itself does not use it.
 *
 * <p>All I/O uses DuckDB's {@code sqlite} extension via standalone connections
 * ({@code DriverManager.getConnection("jdbc:duckdb:")}), not the shared {@code ConnectionPool}.
 * Read operations go through a single persistent connection returned by
 * {@link #openReadOnlyConnection()}, which the caller owns and must close.
 */
public class DynamicQueueRepository implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DynamicQueueRepository.class);

    static final String ATTACHMENT = "q_reg";

    private final String dbPath;

    public DynamicQueueRepository(String dbPath) {
        this.dbPath = dbPath;
    }

    /**
     * Opens a short-lived standalone DuckDB connection, attaches the SQLite file,
     * and runs DDL (idempotent — all statements use IF NOT EXISTS).
     */
    public synchronized void init() throws SQLException {
        String safePath = dbPath.replace("'", "''");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '" + safePath + "' AS " + ATTACHMENT + " (TYPE sqlite)");
            st.execute("""
                CREATE TABLE IF NOT EXISTS %s.ingestion_queues (
                    ingestion_queue      TEXT PRIMARY KEY,
                    catalog              TEXT NOT NULL,
                    schema_name          TEXT NOT NULL,
                    table_name           TEXT NOT NULL,
                    transformation       TEXT,
                    view_name            TEXT,
                    input_table          TEXT,
                    input_schema         TEXT,
                    partition_by         TEXT,
                    num_partitions       INTEGER,
                    partition_expression TEXT,
                    min_bucket_size      INTEGER,
                    max_delay_ms         INTEGER,
                    variables_view       TEXT,
                    variables_key_column TEXT,
                    variables_value_column TEXT,
                    variables_expiration_column TEXT
                )""".formatted(ATTACHMENT));
            // Migrate registries created before the partitioning columns existed. SQLite's
            // ADD COLUMN has no IF NOT EXISTS, so probe the existing columns first and only add
            // the missing ones (a failed ALTER would otherwise close the shared statement).
            java.util.Set<String> existing = existingColumns(conn);
            if (!existing.contains("num_partitions")) {
                st.execute("ALTER TABLE " + ATTACHMENT + ".ingestion_queues ADD COLUMN num_partitions INTEGER");
            }
            if (!existing.contains("partition_expression")) {
                st.execute("ALTER TABLE " + ATTACHMENT + ".ingestion_queues ADD COLUMN partition_expression TEXT");
            }
            for (String column : VARIABLES_COLUMNS) {
                if (!existing.contains(column)) {
                    st.execute("ALTER TABLE " + ATTACHMENT + ".ingestion_queues ADD COLUMN " + column + " TEXT");
                }
            }
            st.execute("""
                CREATE TABLE IF NOT EXISTS %s.schema_version (
                    id      INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL DEFAULT 0
                )""".formatted(ATTACHMENT));
            // ON CONFLICT not supported for SQLite ATTACH targets — use WHERE NOT EXISTS
            st.execute("INSERT INTO " + ATTACHMENT + ".schema_version (id, version) " +
                       "SELECT 1, 0 WHERE NOT EXISTS " +
                       "(SELECT 1 FROM " + ATTACHMENT + ".schema_version WHERE id = 1)");
        }
        logger.debug("SQLite ingestion repository initialised at {}", dbPath);
    }

    /**
     * Columns naming a queue's session-variable relation, added to registries created before they
     * existed. All TEXT: a relation path and the column names inside it (see
     * {@link IngestionVariables}).
     */
    private static final java.util.List<String> VARIABLES_COLUMNS = java.util.List.of(
            "variables_view", "variables_key_column", "variables_value_column",
            "variables_expiration_column");

    /** Lower-cased column names currently on {@code ingestion_queues}, via its own statement. */
    private static java.util.Set<String> existingColumns(Connection conn) throws SQLException {
        java.util.Set<String> cols = new java.util.HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM " + ATTACHMENT + ".ingestion_queues LIMIT 0")) {
            var meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                cols.add(meta.getColumnName(i).toLowerCase(java.util.Locale.ROOT));
            }
        }
        return cols;
    }

    /**
     * Opens a persistent standalone DuckDB connection with the SQLite file attached read-only.
     * The caller owns the returned connection and must close it when done.
     *
     * <p>This single connection is intended to be kept open by {@link DynamicIngestionHandler}
     * for the duration of its lifetime, and passed to {@link #loadAll(Connection)} and
     * {@link #readSchemaVersion(Connection)} on each reload.
     */
    public Connection openReadOnlyConnection() throws SQLException {
        String safePath = dbPath.replace("'", "''");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '" + safePath + "' AS " + ATTACHMENT + " (TYPE sqlite, READ_ONLY true)");
        } catch (SQLException e) {
            try { conn.close(); } catch (Exception ignored) {}
            throw e;
        }
        return conn;
    }

    // -----------------------------------------------------------------------
    // Change detection — file mtime replaces PRAGMA data_version
    // -----------------------------------------------------------------------

    /**
     * Returns the last-modified time of the SQLite file in milliseconds.
     * Used by {@link DynamicIngestionHandler} to detect writes by an external writer.
     * Returns -1 on error or if the file does not exist.
     */
    public static long readDataVersion(String dbPath) {
        try {
            return Files.getLastModifiedTime(Path.of(dbPath)).toMillis();
        } catch (IOException e) {
            logger.debug("Cannot read mtime for {}: {}", dbPath, e.getMessage());
            return -1L;
        }
    }

    // -----------------------------------------------------------------------
    // Read operations — use the persistent connection from openReadOnlyConnection()
    // -----------------------------------------------------------------------

    /**
     * Change-detection query: a primary-key point lookup on the single-row {@code schema_version}
     * table (so no index beyond the implicit PK is needed).
     */
    private static final String SCHEMA_VERSION_QUERY =
            "SELECT version FROM " + ATTACHMENT + ".schema_version WHERE id = 1";

    /**
     * Reads the writer's remote-sync version from {@code schema_version} via the supplied
     * connection (which must have {@value ATTACHMENT} already attached).
     * Returns -1 if unreadable.
     *
     * <p>For a hot polling loop prefer {@link #prepareSchemaVersionQuery(Connection)} +
     * {@link #readSchemaVersion(PreparedStatement)} to avoid re-parsing the query on every poll.
     */
    public static long readSchemaVersion(Connection conn) {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(SCHEMA_VERSION_QUERY)) {
            return rs.next() ? rs.getLong(1) : -1L;
        } catch (SQLException e) {
            logger.debug("schema_version not readable ({}), returning -1", e.getMessage());
            return -1L;
        }
    }

    /**
     * Prepares the {@code schema_version} change-detection query on {@code conn} (which must have
     * {@value ATTACHMENT} attached). Reuse the returned statement across polls to skip re-parsing
     * the query each time; the caller owns it and must close it. Not thread-safe — drive it from a
     * single poller thread.
     */
    public static PreparedStatement prepareSchemaVersionQuery(Connection conn) throws SQLException {
        return conn.prepareStatement(SCHEMA_VERSION_QUERY);
    }

    /**
     * Reads {@code schema_version} via a statement from {@link #prepareSchemaVersionQuery}.
     * Returns -1 if unreadable.
     */
    public static long readSchemaVersion(PreparedStatement schemaVersionQuery) {
        try (ResultSet rs = schemaVersionQuery.executeQuery()) {
            return rs.next() ? rs.getLong(1) : -1L;
        } catch (SQLException e) {
            logger.debug("schema_version not readable ({}), returning -1", e.getMessage());
            return -1L;
        }
    }

    /**
     * Loads all rows from {@code ingestion_queues} via the supplied connection
     * (which must have {@value ATTACHMENT} already attached).
     *
     * <p>The write location and partition columns are <strong>not</strong> read from the registry —
     * {@link DynamicIngestionHandler} (via {@link DuckLakeIngestionHandler}) derives them from the
     * DuckLake table's own metadata. The registry supplies {@code catalog}/{@code schema}/
     * {@code table} (plus optional {@code transformation}/{@code view}/{@code input_table} and
     * {@code input_schema}, the latter used only by {@code manageTables} to derive the table columns);
     * the {@code partition_by} column is reserved for future use and not read here. The
     * {@code num_partitions}/{@code partition_expression} columns split a queue into hash-routed
     * sub-queues (see {@link PartitionedIngestionQueue}). The {@code variables_*} columns name a
     * key/value relation holding the queue's session variables (see {@link IngestionVariables}).
     */
    public static Map<String, QueueIdToTableMapping> loadAll(Connection conn) throws SQLException {
        Map<String, QueueIdToTableMapping> result = new LinkedHashMap<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT ingestion_queue, catalog, schema_name, table_name," +
                     " transformation, view_name, input_table, input_schema," +
                     " num_partitions, partition_expression, variables_view," +
                     " variables_key_column, variables_value_column, variables_expiration_column" +
                     " FROM " + ATTACHMENT + ".ingestion_queues")) {
            while (rs.next()) {
                String queueId     = rs.getString("ingestion_queue");
                String catalog     = rs.getString("catalog");
                String schema      = rs.getString("schema_name");
                String table       = rs.getString("table_name");
                String transform   = rs.getString("transformation");
                String view        = rs.getString("view_name");
                String inputTable  = rs.getString("input_table");
                String inputSchema = rs.getString("input_schema");
                int numPartitions  = rs.getInt("num_partitions");
                if (rs.wasNull() || numPartitions < 1) {
                    numPartitions = 1; // NULL (legacy row) / unset means "not partitioned"
                }
                String partitionExpr = rs.getString("partition_expression");
                // outputPath omitted: derived from DuckLake metadata by the handler.
                result.put(queueId, new QueueIdToTableMapping(
                        queueId, catalog, schema, table, Map.of(), transform, view, inputTable)
                        .withInputSchema(inputSchema)
                        .withPartitioning(numPartitions, partitionExpr)
                        .withVariables(variablesOf(queueId, rs)));
            }
        }
        return result;
    }

    /**
     * The queue's session variables as the registry describes them: a relation to read them from,
     * or {@link IngestionVariables#NONE} when the row names none. Static pairs are not stored here
     * — a registry-driven deployment changes values by writing to that relation, which is the point
     * of running this provider.
     *
     * <p>The relation is named by its full path, so it can be a table in this very SQLite file
     * (attached again on the ingestion connection by the startup script), a DuckLake view, or
     * anything else that connection can read.
     */
    private static IngestionVariables variablesOf(String queueId, ResultSet rs) throws SQLException {
        String relation = rs.getString("variables_view");
        if (relation == null || relation.isBlank()) {
            return IngestionVariables.NONE;
        }
        String keyColumn = columnOr(rs, "variables_key_column", "key");
        String valueColumn = columnOr(rs, "variables_value_column", "value");
        String expiration = rs.getString("variables_expiration_column");
        try {
            return new IngestionVariables(Map.of(), new IngestionVariables.View(
                    relation.trim(), keyColumn, valueColumn,
                    expiration == null || expiration.isBlank() ? null : expiration.trim()));
        } catch (IllegalArgumentException e) {
            // One bad row must not take the whole registry down with it: the reload would fail and
            // every other queue would keep running on stale mappings, with nothing saying why.
            logger.warn("Queue '{}': ignoring its variables, the registry row is not usable: {}",
                    queueId, e.getMessage());
            return IngestionVariables.NONE;
        }
    }

    /** A column's trimmed value, or {@code fallback} when the row leaves it unset. */
    private static String columnOr(ResultSet rs, String column, String fallback) throws SQLException {
        String value = rs.getString(column);
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    @Override
    public void close() {
        // init() uses a try-with-resources connection — nothing to close here.
    }
}
