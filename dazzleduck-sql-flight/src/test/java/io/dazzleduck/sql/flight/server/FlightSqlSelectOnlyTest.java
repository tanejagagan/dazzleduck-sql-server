package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for READ_ONLY mode.
 * Validates that only SELECT queries are allowed and all DML/DDL operations are blocked.
 */
public class FlightSqlSelectOnlyTest {

    private static final String TEST_USER = "admin";
    private static final String TEST_CATALOG = "memory";
    private static final String TEST_SCHEMA = "main";
    private static ServerClient serverClient;
    private static Location serverLocation;

    @BeforeAll
    public static void setup() throws Exception {
        // Create test data directory
        var testDataPath = Files.createTempDirectory("readonly_test_").toString();

        // Create test parquet file
        Path testFile = Path.of(testDataPath, "data.parquet");
        ConnectionPool.execute("COPY (SELECT 1 as id, 'test' as name) TO '%s' (FORMAT parquet)".formatted(testFile.toString()));

        // Setup READ_ONLY server
        serverLocation = FlightTestUtils.findNextLocation();
        var flightTestUtils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);
        serverClient = flightTestUtils.createReadOnlyServerClient(serverLocation);
    }

    @AfterAll
    public static void cleanup() throws Exception {
        if (serverClient != null) {
            serverClient.close();
        }
    }

    // ===== SELECT Operations (Should be ALLOWED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectAllowed() throws Exception {
        // Basic SELECT should work
        String query = "SELECT 1";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithWhereAllowed() throws Exception {
        // SELECT with WHERE clause should work
        String query = "SELECT * FROM generate_series(10) AS t(value) WHERE value > 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithJoinAllowed() throws Exception {
        // SELECT with JOIN should work
        String query = "SELECT * FROM generate_series(5) AS a(value) JOIN generate_series(5) AS b(value) ON a.value = b.value";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithUnionAllowed() throws Exception {
        // SELECT with UNION should work
        String query = "SELECT 1 UNION ALL SELECT 2";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithCTEAllowed() throws Exception {
        // SELECT with CTE should work
        String query = "WITH cte AS (SELECT 1) SELECT * FROM cte";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithAggregateAllowed() throws Exception {
        // SELECT with aggregate functions should work
        String query = "SELECT COUNT(*) FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithGroupByAllowed() throws Exception {
        // SELECT with GROUP BY should work
        String query = "SELECT value % 2, COUNT(*) FROM generate_series(10) AS t(value) GROUP BY (value % 2)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithWindowFunctionAllowed() throws Exception {
        // SELECT with window function should work
        String query = "SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    // ===== DML Operations (Should be BLOCKED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInsertBlocked() {
        // INSERT should be blocked
        String query = "INSERT INTO test_table VALUES (1, 'test')";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUpdateBlocked() {
        // UPDATE should be blocked
        String query = "UPDATE test_table SET name = 'updated' WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDeleteBlocked() {
        // DELETE should be blocked
        String query = "DELETE FROM test_table WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateTableBlocked() {
        // CREATE TABLE should be blocked
        String query = "CREATE TABLE new_table (id INT, name VARCHAR)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropTableBlocked() {
        // DROP TABLE should be blocked
        String query = "DROP TABLE IF EXISTS test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAlterTableBlocked() {
        // ALTER TABLE should be blocked
        String query = "ALTER TABLE test_table ADD COLUMN new_col VARCHAR";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTruncateBlocked() {
        // TRUNCATE TABLE should be blocked
        String query = "TRUNCATE TABLE test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateSchemaBlocked() {
        // CREATE SCHEMA should be blocked
        String query = "CREATE SCHEMA IF NOT EXISTS new_schema";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropSchemaBlocked() {
        // DROP SCHEMA should be blocked
        String query = "DROP SCHEMA IF EXISTS new_schema";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateViewBlocked() {
        // CREATE VIEW should be blocked
        String query = "CREATE VIEW test_view AS SELECT 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropViewBlocked() {
        // DROP VIEW should be blocked
        String query = "DROP VIEW IF EXISTS test_view";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testGrantBlocked() {
        // GRANT should be blocked
        String query = "GRANT SELECT ON test_table TO user";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRevokeBlocked() {
        // REVOKE should be blocked
        String query = "REVOKE SELECT ON test_table FROM user";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateIndexBlocked() {
        // CREATE INDEX should be blocked
        String query = "CREATE INDEX idx_name ON test_table (column_name)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDropIndexBlocked() {
        // DROP INDEX should be blocked
        String query = "DROP INDEX IF EXISTS idx_name ON test_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCopyFromBlocked() {
        // COPY FROM should be blocked (data import)
        String query = "COPY test_table FROM '/path/to/file.csv'";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testExportToBlocked() {
        // EXPORT TO should be blocked (data export)
        String query = "EXPORT DATABASE 'memory' TO '/path/to/export.sql'";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    // ===== Mixed Operations (Should be BLOCKED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInsertSelectBlocked() {
        // INSERT INTO ... SELECT should be blocked
        String query = "INSERT INTO target_table SELECT * FROM source_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCreateTableAsSelectBlocked() {
        // CREATE TABLE AS SELECT should be blocked
        String query = "CREATE TABLE new_table AS SELECT * FROM source_table";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUpdateWithSubqueryBlocked() {
        // UPDATE with subquery should be blocked
        String query = "UPDATE test_table SET col = (SELECT MAX(col) FROM test_table) WHERE id = 1";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDeleteWithSubqueryBlocked() {
        // DELETE with subquery should be blocked
        String query = "DELETE FROM test_table WHERE id IN (SELECT id FROM other_table)";

        assertThrows(FlightRuntimeException.class, () ->
            serverClient.flightSqlClient().execute(query));
    }



    // ===== Complex SELECT Operations (Should be ALLOWED) =====

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithSubqueryAllowed() throws Exception {
        // SELECT with subquery should work
        String query = "SELECT * FROM (SELECT 1 AS id, 'test' AS name)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithInSubqueryAllowed() throws Exception {
        // SELECT with IN subquery should work
        String query = "SELECT * FROM generate_series(10) AS t(value) WHERE value IN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithCaseAllowed() throws Exception {
        // SELECT with CASE should work
        String query = "SELECT value, CASE WHEN value > 5 THEN 'high' ELSE 'low' END as level FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithDistinctAllowed() throws Exception {
        // SELECT with DISTINCT should work
        String query = "SELECT DISTINCT value % 2 FROM generate_series(10) AS t(value)";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithLimitAllowed() throws Exception {
        // SELECT with LIMIT should work
        String query = "SELECT * FROM generate_series(10) AS t(value) LIMIT 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithOffsetAllowed() throws Exception {
        // SELECT with OFFSET should work
        String query = "SELECT * FROM generate_series(10) AS t(value) OFFSET 5";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSelectWithOrderByAllowed() throws Exception {
        // SELECT with ORDER BY should work
        String query = "SELECT * FROM generate_series(10) AS t(value) ORDER BY value DESC";
        FlightTestUtils.testQuery(query, query, serverClient.flightSqlClient(), serverClient.clientAllocator());
    }
}
