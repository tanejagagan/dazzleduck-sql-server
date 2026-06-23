package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Locks in a property the whole ingestion path depends on: {@code ducklake_add_data_files} matches a
 * Parquet file's columns to the target table <strong>by name, not by position</strong>.
 *
 * <p>The collector writes {@code SELECT *}-shaped Parquet (whose column order can vary with the
 * transformation) and registers it with {@code ignore_extra_columns => true}; correctness relies on
 * DuckLake aligning columns by name. This test registers two Parquet files with opposite column order
 * — {@code (key, value)} and {@code (value, key)} — into a table declared {@code (key, value)} and
 * asserts both files' rows land with the values correctly aligned.
 */
class DuckLakeColumnOrderTest {

    @TempDir Path tempDir;
    static final String CATALOG = "column_order_lake";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        Files.createDirectories(tempDir.resolve("data").resolve("main").resolve("t"));
        ConnectionPool.execute("INSTALL arrow FROM community");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.main.t (key INTEGER, value VARCHAR)".formatted(CATALOG)
            });
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void addsFilesWithDifferentColumnOrderMatchedByName() throws Exception {
        Path fileKV = tempDir.resolve("kv.parquet");   // declared (key, value)
        Path fileVK = tempDir.resolve("vk.parquet");   // declared (value, key)

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "COPY (SELECT 1 AS key, 'a' AS value) TO '%s' (FORMAT parquet)".formatted(fileKV));
            ConnectionPool.execute(conn,
                    "COPY (SELECT 'b' AS value, 2 AS key) TO '%s' (FORMAT parquet)".formatted(fileVK));

            // Guard the test's premise: the two files really have opposite column order.
            assertEquals(List.of("key", "value"), describeParquet(conn, fileKV));
            assertEquals(List.of("value", "key"), describeParquet(conn, fileVK));

            String add = "CALL ducklake_add_data_files('%s', 't', '%s', schema => 'main', "
                    + "ignore_extra_columns => true, allow_missing => true)";
            ConnectionPool.execute(conn, add.formatted(CATALOG, fileKV));
            ConnectionPool.execute(conn, add.formatted(CATALOG, fileVK));
        }

        try (Connection conn = ConnectionPool.getConnection()) {
            List<String> rows = new ArrayList<>();
            ConnectionPool.collectAll(conn,
                    "SELECT key, value FROM %s.main.t ORDER BY key".formatted(CATALOG),
                    rs -> rs.getInt("key") + "|" + rs.getString("value")).forEach(rows::add);
            // Both files' rows land with key/value matched by name regardless of file column order.
            assertEquals(List.of("1|a", "2|b"), rows);
        }
    }

    private static List<String> describeParquet(Connection conn, Path file) throws Exception {
        List<String> cols = new ArrayList<>();
        ConnectionPool.collectAll(conn,
                "DESCRIBE SELECT * FROM read_parquet('%s')".formatted(file),
                rs -> rs.getString("column_name")).forEach(cols::add);
        return cols;
    }
}
