package io.dazzleduck.sql.commons.ducklake;

import io.dazzleduck.sql.commons.ConnectionPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Shared test fixture for DuckLake tests.
 * Sets up a partitioned/non-partitioned table with a standard schema
 * and supports manually adding parquet files to test ducklake_add_data_files.
 *
 * Table schema: key STRING, value STRING, partition INT, version INT
 * Partitioned by: partition column (when partitioned=true)
 */
public class DuckLakeTestFixture {

    /**
     * Returns SQL statements to create and populate a test table.
     * Schema: key STRING, value STRING, partition INT, version INT
     * When partitioned=true, sets PARTITIONED BY (partition) after the initial insert.
     */
    public static String[] getTableStatements(String table, boolean partitioned) {
        var createStatement = "CREATE TABLE %s(key string, value string, partition int)".formatted(table);
        var insertStatement0 = "INSERT INTO %s VALUES ('k00', 'v00', 0), ('k01', 'v01', 0)".formatted(table);
        var partitionTable = partitioned ? "ALTER TABLE %s SET PARTITIONED BY (partition)".formatted(table)
                : "select 1";
        var insertStatement1 = "INSERT INTO %s VALUES (null, null, 1)".formatted(table);
        var insertStatement2 = "INSERT INTO %s VALUES ('k11', 'v11', 1), ('k21', 'v21', 1)".formatted(table);
        var insertStatement3 = "INSERT INTO %s VALUES ('k31', 'v31', 1), ('k41', 'v41', 1)".formatted(table);
        var insertStatement4 = "INSERT INTO %s VALUES ('k51', 'v51', 1), ('k61', 'v61', 1)".formatted(table);
        var addColumn = "ALTER TABLE %s ADD COLUMN version INT".formatted(table);
        var insertStatement5 = "INSERT INTO %s VALUES ('k51', 'v51', 1, 0), ('k61', 'v61', 1, 0)".formatted(table);
        var insertStatement6 = "INSERT INTO %s VALUES ('k72', 'v72', 2, 0), ('k82', 'v82', 2, 0)".formatted(table);
        return new String[]{
                createStatement,
                insertStatement0,
                partitionTable,
                insertStatement1,
                insertStatement2,
                insertStatement3,
                insertStatement4,
                addColumn,
                insertStatement5,
                insertStatement6,
        };
    }

    /**
     * Creates a test table using ConnectionPool.
     */
    public static void createTestTable(String table, boolean partitioned) {
        for (var statement : getTableStatements(table, partitioned)) {
            ConnectionPool.execute(statement);
        }
    }

    /**
     * Manually adds parquet files to the DuckLake table via ducklake_add_data_files.
     *
     * For partitioned tables (DuckLake v1.4.4+):
     *   - Files must be written into partition subdirectories (e.g. partition=9/)
     *   - The partition column must NOT appear in the parquet file; DuckLake derives it from the path
     *   - Adds two files: partition=9 and partition=8 (total file count increases by 2)
     *
     * For non-partitioned tables:
     *   - Writes a single file with all columns including partition and version
     *
     * @param catalog   DuckLake catalog name (e.g. "my_ducklake")
     * @param dataBase  Base path of the table data directory (e.g. "/tmp/data/main/tt_p")
     * @param table     Unqualified table name (e.g. "tt_p")
     * @param partitioned whether the table is partitioned
     */
    public static void addDataFile(String catalog, String dataBase, String table, boolean partitioned)
            throws IOException {
        if (partitioned) {
            String path9 = dataBase + "/partition=9/manual.parquet";
            String path8 = dataBase + "/partition=8/manual.parquet";
            Files.createDirectories(Path.of(dataBase + "/partition=9"));
            Files.createDirectories(Path.of(dataBase + "/partition=8"));
            ConnectionPool.executeBatch(new String[]{
                    "COPY (SELECT 'k119' AS key, 'v119' AS value, NULL::INT AS version) TO '%s' (FORMAT parquet)".formatted(path9),
                    "COPY (SELECT 'k118' AS key, 'v118' AS value, NULL::INT AS version) TO '%s' (FORMAT parquet)".formatted(path8),
                    "CALL ducklake_add_data_files('%s', '%s', '%s')".formatted(catalog, table, path9),
                    "CALL ducklake_add_data_files('%s', '%s', '%s')".formatted(catalog, table, path8)
            });
        } else {
            String path = dataBase + "/manual.parquet";
            ConnectionPool.executeBatch(new String[]{
                    "COPY (SELECT * FROM (VALUES('k119', 'v119', 9, NULL::INT),('k118', 'v118', 8, NULL::INT)) AS t(key, value, partition, version)) TO '%s' (FORMAT parquet)".formatted(path),
                    "CALL ducklake_add_data_files('%s', '%s', '%s')".formatted(catalog, table, path)
            });
        }
    }
}
