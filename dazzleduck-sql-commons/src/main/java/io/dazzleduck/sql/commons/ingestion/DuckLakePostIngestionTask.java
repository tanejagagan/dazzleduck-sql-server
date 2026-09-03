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
import java.util.stream.Collectors;

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
 * <p>When the spec also sets {@code watermark_snapshot_id_column}, the snapshot id is written by
 * that same INSERT, so it commits with the rows rather than being patched in afterwards. DuckLake
 * does not expose the pending snapshot id, but it derives it as {@code max(snapshot_id) + 1} and
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
            Long predictedSnapshotId = hasWatermark && watermarkSpec.snapshotIdColumn() != null
                    ? predictNextSnapshotId(conn)
                    : null;
            if (hasWatermark) {
                queries.add(watermarkSpec.insertSql(catalogName, schemaName, watermarkRows, predictedSnapshotId));
            }
            ConnectionPool.executeBatchInTxn(conn, queries.toArray(String[]::new));
            if (predictedSnapshotId != null) {
                verifySnapshotId(conn, files, predictedSnapshotId);
            }
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
     * retries our commit internally at a higher id, transparently and without error. That is rare,
     * so the common path is a single transaction carrying the correct id; this is the safety net
     * that keeps a lost race from silently persisting a wrong one.
     *
     * <p>Failures here are logged rather than thrown — the batch is already durable, and failing an
     * ingest that succeeded would be worse than a stale id.
     */
    private void verifySnapshotId(Connection conn, List<String> files, long predicted) {
        try {
            Long actual = resolveCommittedSnapshotId(conn, files, predicted);
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

    /**
     * The snapshot the just-registered files became visible in. All files of one batch are
     * registered by a single transaction, so exactly one distinct {@code begin_snapshot} is
     * expected; anything else means the batch did not commit as one unit and is not stamped.
     */
    private Long resolveCommittedSnapshotId(Connection conn, List<String> files, long lowerBound) throws SQLException {
        String paths = files.stream().map(f -> "'" + escapeLiteral(f) + "'").collect(Collectors.joining(", "));
        // begin_snapshot >= lowerBound is free pruning, not a filter on correctness: the committed id
        // is always >= the predicted one (DuckLake takes max+1 at commit, and max only grows), and
        // begin_snapshot rises with time, so this lets zone maps skip nearly the whole table. Without
        // it this is an unindexed scan of every path in the catalog — 3.4ms vs 0.3ms at 1M files.
        String query = ("SELECT count(DISTINCT begin_snapshot) AS distinct_snapshots, max(begin_snapshot) AS snapshot_id "
                + "FROM __ducklake_metadata_%s.ducklake_data_file WHERE begin_snapshot >= %d AND path IN (%s)")
                .formatted(escapeLiteral(catalogName), lowerBound, paths);
        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (!resultSet.next() || resultSet.getLong("distinct_snapshots") != 1) {
                logger.warn("Expected exactly one snapshot for the {} files registered for queue '{}'; "
                        + "leaving '{}' NULL", files.size(), ingestionResult.queueName(), watermarkSpec.snapshotIdColumn());
                return null;
            }
            return resultSet.getLong("snapshot_id");
        }
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }
}
