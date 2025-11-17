package io.dazzleduck.sql.commons.ducklake;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning.constructQuery;

public class DuckLakePartitionPruningTest {

    public static String WORKSPACE;
    public static final String DATABASE = "test_ducklake_partition";
    public static final String METADATA_DATABASE = "__ducklake_metadata_%s".formatted(DATABASE);
    public static final String FILE_COLUMN_STAT_TABLE = "%s.ducklake_file_column_stats".formatted(METADATA_DATABASE);
    public static final String PARTITIONED_TABLE = "t_partitioned";
    public static final String NO_PARTITIONED_TABLE = "t";
    public static final String QUALIFIED_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, PARTITIONED_TABLE);
    public static final String QUALIFIED_NO_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, NO_PARTITIONED_TABLE);
    public static final String COLUMN_MAPPING_TABLE = "ducklake_column_mapping";
    public static final String QUALIFIED_COLUMN_MAPPING_TABLE = "%s.%s".formatted(METADATA_DATABASE, COLUMN_MAPPING_TABLE);

    static {
        try {
            WORKSPACE = Files.createTempDirectory("my-temp-dir-").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setup() throws IOException {
        String[] setups = {
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(WORKSPACE, DATABASE, WORKSPACE),
        };
        ConnectionPool.executeBatch(setups);
        createTestTable(QUALIFIED_PARTITIONED_TABLE, true);
        addDataFile(PARTITIONED_TABLE);
        createTestTable(QUALIFIED_NO_PARTITIONED_TABLE, false);
        addDataFile(NO_PARTITIONED_TABLE);
        ConnectionPool.printResult("SELECT * FROM %s".formatted(QUALIFIED_PARTITIONED_TABLE));
        ConnectionPool.printResult("SELECT * FROM %s".formatted(QUALIFIED_NO_PARTITIONED_TABLE));
        ConnectionPool.printResult("SELECT * FROM %s".formatted(FILE_COLUMN_STAT_TABLE));
        ConnectionPool.printResult("SELECT * FROM %s".formatted(QUALIFIED_COLUMN_MAPPING_TABLE));

    }

    private static void addDataFile(String table) {
        String path = WORKSPACE + "/data/main/" + table + "/manual.parquet";
        String sql = "COPY (SELECT * FROM (VALUES('k119', 'v119', 9),('k118', 'v118', 8)) AS my_value(key, value, partition)) TO '%s' (FORMAT parquet)".formatted(path);
        String addTableSql = "CALL ducklake_add_data_files('%s', '%s', '%s', allow_missing => true)".formatted(DATABASE, table, path);
        String[] batch = {sql, addTableSql};
        ConnectionPool.executeBatch(batch);
    }

    @Test
    public void testPartitioned() {
        String query = constructQuery(METADATA_DATABASE, 1, List.of(1L, 2L, 3L, 4L));
        ConnectionPool.printResult(query);
    }

    @Test
    public void testUnPartitioned() {


    }

    public static void createTestTable(String table , boolean partitioned) {
        String[] statements = getStatements(table, partitioned);
        ConnectionPool.executeBatch(statements);
    }

    @NotNull
    private static String[] getStatements(String table, boolean partition) {
        var createStatement = "CREATE TABLE %s(key string, value string, partition int)".formatted( table);
        var partitionTable = partition ? "ALTER TABLE %s SET PARTITIONED BY (partition)".formatted(table)
                : "select 1";
        var insertStatement0 = "INSERT INTO %s VALUES (null, null, 1)".formatted(table);
        var insertStatement1 = "INSERT INTO %s VALUES ('k11', 'v11', 1), ('k21', 'v21', 1) ".formatted(table);
        var insertStatement2 = "INSERT INTO %s VALUES ('k31', 'v31', 1), ('k41', 'v41', 1) ".formatted(table);
        var insertStatement3 = "INSERT INTO %s VALUES ('k51', 'v51', 1), ('k61', 'v61', 1) ".formatted(table);
        var addColumn = "ALTER TABLE %s ADD COLUMN version VARCHAR".formatted(table);
        return new String[]{
                createStatement,
                partitionTable,
                insertStatement0,
                insertStatement1,
                insertStatement2,
                insertStatement3,
                addColumn
        };
    }

    @Test
    public void testTransformation() {
        String table = "ducklake_file_column_stats";
        String insertStatement = "INSERT INTO ";
        String create = "CREATE TABLE %s (table_id bigint, column_id )";
        //String createPartitionTable = "CREATE TABLE %s AS SELECT * FROM %s".formatted(table);
    }

    public static void createNonPartitionedTable() {
        // create table
        // add rows with nulls
        // add column
        // add rows with few null
        // add external parquet file
    }

    public static void createPartitionedTable() {
        // create table
        // add rows with nulls
        // add column
        // add rows with few null
        // add external parquet file
    }
}
