package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Post-ingestion task that adds newly ingested files to a DuckLake table.
 * This task executes the ducklake_add_data_files procedure for each ingested file
 * within a transaction to ensure atomicity.
 *
 * <p>When the queue mapping configures a watermark (see {@link WatermarkSpec}), the rows
 * precomputed at write time and carried on {@link IngestionResult#watermarkRows()} are appended
 * to the watermark table via a plain {@code INSERT ... VALUES} in the SAME transaction, so file
 * registration and watermark commit or roll back together. This task never re-reads the written
 * files.
 *
 * <p>The snapshot id ({@code watermark_snapshot_id_column}) is written by that same INSERT, so it
 * commits with the rows rather than being patched in afterwards. DuckLake does not expose the
 * pending snapshot id, but it derives it as {@code max(snapshot_id) + 1} and
 * enforces it with a primary key, so the value can be computed up front (see
 * {@link #predictNextSnapshotId}) and is guaranteed correct unless a competing writer takes the id
 * first — in which case DuckLake retries our commit one higher, and
 * {@link #verifySnapshotId} repairs the rows.
 *
 * <p>Limitation: queues registered through the dynamic SQLite registry
 * ({@link DynamicQueueRepository}) do not carry {@code additional_parameters}, so watermarks are
 * only available for statically configured queue mappings.
 */
public class DuckLakePostIngestionTask implements PostIngestionTask {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTask.class);

    private static final String ADD_FILE_QUERY = "CALL ducklake_add_data_files('%s', '%s', '%s', schema => '%s', ignore_extra_columns => true, allow_missing => true);";

    private final IngestionResult ingestionResult;
    private final String catalogName;
    private final String tableName;
    private final String schemaName;
    private final WatermarkSpec watermarkSpec;

    public DuckLakePostIngestionTask(IngestionResult ingestionResult,
                                     String catalogName,
                                     String tableName,
                                     String schemaName,
                                     Map<String, String> additionalParameters) {
        this.ingestionResult = ingestionResult;
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.watermarkSpec = WatermarkSpec.fromParameters(ingestionResult.queueName(), additionalParameters);
    }

    @Override
    public void execute() {
        List<String> files = ingestionResult.filesCreated();
        if (files == null || files.isEmpty()) {
            logger.debug("No files to add to DuckLake for catalog={}, table={}", catalogName, tableName);
            return;
        }

        try {
            addFilesInTransaction(files);
            logger.info("Successfully added {} files to DuckLake table {}.{}.{}", files.size(), catalogName, schemaName, tableName);
        } catch (SQLException e) {
            logger.error("Failed to add files to DuckLake table {}.{}.{}", catalogName, schemaName, tableName, e);
            throw new RuntimeException("Failed to execute DuckLake post-ingestion task for table " + tableName, e);
        }
    }

    /**
     * Adds files to DuckLake table within a transaction.
     * All files are added atomically - if any file fails, all changes are rolled back.
     * Precomputed watermark rows join the same transaction, so the registered files and their
     * watermark commit or roll back together.
     */
    private void addFilesInTransaction(List<String> files) throws SQLException {
        List<String> queries = new ArrayList<>(files.stream()
                .map(file -> ADD_FILE_QUERY.formatted(
                        escapeLiteral(catalogName), escapeLiteral(tableName), escapeLiteral(file), escapeLiteral(schemaName)))
                .toList());
        List<List<String>> watermarkRows = ingestionResult.watermarkRows();
        boolean hasWatermark = watermarkSpec != null && watermarkRows != null && !watermarkRows.isEmpty();
        try (Connection conn = ConnectionPool.getConnection()) {
            if (!hasWatermark) {
                ConnectionPool.executeBatchInTxn(conn, queries.toArray(String[]::new));
                return;
            }
            long predictedSnapshotId = predictNextSnapshotId(conn);
            queries.add(watermarkSpec.insertSql(catalogName, schemaName, watermarkRows, predictedSnapshotId));
            ConnectionPool.executeBatchInTxn(conn, queries.toArray(String[]::new));
            verifySnapshotId(conn, files, predictedSnapshotId);
        }
    }

    /**
     * The id this transaction will commit as, barring a competing writer: DuckLake derives the next
     * snapshot id the same way and enforces it with a primary key on {@code ducklake_snapshot}.
     */
    private long predictNextSnapshotId(Connection conn) throws SQLException {
        String query = "SELECT coalesce(max(snapshot_id), -1) + 1 AS next_id FROM __ducklake_metadata_%s.ducklake_snapshot"
                .formatted(escapeLiteral(catalogName));
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            resultSet.next();
            return resultSet.getLong("next_id");
        }
    }

    /**
     * Confirms the transaction actually committed as the predicted snapshot, and repairs the rows
     * if not.
     *
     * <p>The prediction only loses when a competing writer takes the id first: DuckLake then
     * retries our commit internally at a higher id, transparently and without error.
     *
     * <p>The common case is settled without touching {@code ducklake_data_file} at all. The
     * committed id is always {@code >=} the predicted one, and {@code current_snapshot()} is the
     * newest id in the catalog, so {@code current == predicted} can only mean our commit took
     * exactly the predicted id. Only when the catalog has moved further does the file lookup run —
     * which is also the only case where a repair could be needed.
     *
     * <p>Failures here are logged rather than thrown — the batch is already durable, and failing an
     * ingest that succeeded would be worse than a stale id.
     */
    private void verifySnapshotId(Connection conn, List<String> files, long predicted) {
        try {
            if (currentSnapshotId(conn) == predicted) {
                return;
            }
            Long actual = resolveCommittedSnapshotId(conn, files.get(0), predicted);
            if (actual == null || actual == predicted) {
                return;
            }
            logger.info("Queue '{}' committed as snapshot {} rather than the predicted {} (a concurrent "
                    + "writer took the id); correcting the watermark rows", ingestionResult.queueName(), actual, predicted);
            ConnectionPool.execute(conn, watermarkSpec.updateSnapshotIdSql(catalogName, schemaName, actual));
        } catch (Exception e) {
            logger.warn("Could not verify the snapshot id written to {}.{}.{}; rows may carry the predicted {} "
                    + "instead of the committed id", catalogName, schemaName, watermarkSpec.table(), predicted, e);
        }
    }

    /** Newest snapshot in the catalog, i.e. an upper bound on the id this batch committed as. */
    private long currentSnapshotId(Connection conn) throws SQLException {
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT id FROM ducklake_current_snapshot('%s')".formatted(escapeLiteral(catalogName)))) {
            resultSet.next();
            return resultSet.getLong("id");
        }
    }

    /**
     * The snapshot a registered file became visible in. One file is enough: the whole batch is
     * registered by a single transaction, so every file of it carries the same
     * {@code begin_snapshot}.
     *
     * <p>{@code begin_snapshot >= lowerBound} is free pruning rather than a filter on correctness —
     * the committed id is always {@code >=} the predicted one, and {@code begin_snapshot} rises with
     * time, so zone maps skip almost the whole table instead of scanning every path in the catalog.
     */
    private Long resolveCommittedSnapshotId(Connection conn, String file, long lowerBound) throws SQLException {
        String query = ("SELECT begin_snapshot FROM __ducklake_metadata_%s.ducklake_data_file "
                + "WHERE begin_snapshot >= %d AND path = '%s'")
                .formatted(escapeLiteral(catalogName), lowerBound, escapeLiteral(file));
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (!resultSet.next()) {
                logger.warn("Could not locate registered file {} for queue '{}'; leaving '{}' as written",
                        file, ingestionResult.queueName(), watermarkSpec.snapshotIdColumn());
                return null;
            }
            return resultSet.getLong("begin_snapshot");
        }
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }
}
