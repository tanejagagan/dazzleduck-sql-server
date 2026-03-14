package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;


public class DuckLakePartitionPruningTest {

    // Expected file counts after partition pruning
    private static final int EXPECTED_FILES_WITH_FILTER = 2; // Files matching key = 'k52' filter
    private static final int EXPECTED_FILES_NO_FILTER = 9; // Total files without any filter (7 inserts + 2 manual)

    @TempDir
    static Path WORKSPACE;

    public static final String DATABASE = "my_ducklake";
    public static final String METADATA_DATABASE = "__ducklake_metadata_%s".formatted(DATABASE);
    public static final String PARTITIONED_TABLE = "tt_p";
    public static final String NON_PARTITIONED_TABLE = "tt";
    public static final String QUALIFIED_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, PARTITIONED_TABLE);
    public static final String QUALIFIED_NON_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, NON_PARTITIONED_TABLE);

    @BeforeAll
    public static void setup() throws IOException {
        String workspacePath = WORKSPACE.toString();
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspacePath, DATABASE, workspacePath),
        });
        DuckLakeTestFixture.createTestTable(QUALIFIED_PARTITIONED_TABLE, true);
        DuckLakeTestFixture.addDataFile(DATABASE, workspacePath + "/data/main/" + PARTITIONED_TABLE, PARTITIONED_TABLE, true);
        DuckLakeTestFixture.createTestTable(QUALIFIED_NON_PARTITIONED_TABLE, false);
        DuckLakeTestFixture.addDataFile(DATABASE, workspacePath + "/data/main/" + NON_PARTITIONED_TABLE, NON_PARTITIONED_TABLE, false);
    }

    @Test
    public void testTransformation() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where key = 'k52'".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_WITH_FILTER, files.size(), "Expected 2 files matching key = 'k52' filter");
    }

    @Test
    public void testTransformationNoFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 9 files without filter");
    }

    @Test
    public void testTransformationFakeFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where true".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 9 files for 'where true' filter");
    }

    @Test
    public void testTransformationNoConstantsFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where key = value and key = 'k52'".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_WITH_FILTER, files.size(), "Expected 2 files for complex filter with key = 'k52'");
    }

    @Test
    public void testNonExistentTable() {
        var sql = "select * from non_existent_table where key = 'k52'";
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        Assertions.assertThrows(SQLException.class, () -> pruning.pruneFiles("main", "non_existent_table", sql), "Should throw SQLException for non-existent table");
    }

    @Test
    public void testInvalidSQL() {
        var sql = "this is not valid SQL";
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        Assertions.assertThrows(Exception.class, () -> pruning.pruneFiles("main", PARTITIONED_TABLE, sql), "Should throw exception for invalid SQL syntax");
    }
}
