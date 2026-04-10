package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for SelectOnlyAuthorizer
 * Tests that only SELECT and UNION queries are authorized.
 *
 * Note: The DuckDB JSON serialization only supports SELECT statements,
 * so DML/DDL operations (INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
 * cannot be tested here as they fail during parsing before authorization.
 */
public class SelectOnlyAuthorizerTest {

    // ===== SELECT Operations (Should be ALLOWED) =====

    @Test
    public void testBasicSelectAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: Basic SELECT should be allowed ---");
        String sql = "SELECT * FROM test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Basic SELECT was allowed");
    }

    @Test
    public void testSelectWithWhereAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with WHERE should be allowed ---");
        String sql = "SELECT * FROM test_table WHERE id = 1";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ SELECT with WHERE was allowed");
    }

    @Test
    public void testSelectWithOrderByAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with ORDER BY should be allowed ---");
        String sql = "SELECT * FROM test_table ORDER BY name";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ SELECT with ORDER BY was allowed");
    }

    @Test
    public void testSelectWithSubqueryAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with subquery should be allowed ---");
        String sql = "SELECT * FROM (SELECT * FROM test_table) as sub";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ SELECT with subquery was allowed");
    }

    @Test
    public void testSelectWithInSubqueryAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with IN subquery should be allowed ---");
        String sql = "SELECT * FROM test_table WHERE id IN (SELECT id FROM other_table)";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ SELECT with IN subquery was allowed");
    }

    // ===== JOIN Operations (Should be ALLOWED) =====

    @Test
    public void testInnerJoinAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with INNER JOIN should be allowed ---");
        String sql = "SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ INNER JOIN was allowed");
    }

    @Test
    public void testLeftJoinAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with LEFT JOIN should be allowed ---");
        String sql = "SELECT * FROM table1 LEFT JOIN table2 ON table1.id = table2.id";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ LEFT JOIN was allowed");
    }

    @Test
    public void testRightJoinAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with RIGHT JOIN should be allowed ---");
        String sql = "SELECT * FROM table1 RIGHT JOIN table2 ON table1.id = table2.id";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ RIGHT JOIN was allowed");
    }

    @Test
    public void testJoinWithWhereAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with JOIN and WHERE should be allowed ---");
        String sql = "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id WHERE table1.name = 'test'";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ JOIN with WHERE was allowed");
    }

    @Test
    public void testMultipleJoinsAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with multiple JOINs should be allowed ---");
        String sql = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id JOIN table3 t3 ON t1.id = t3.id";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Multiple JOINs were allowed");
    }

    // ===== Complex SELECT Operations (Should be ALLOWED) =====

    @Test
    public void testSelectWithAggregateFunctionsAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with aggregate functions should be allowed ---");
        String sql = "SELECT COUNT(*), SUM(amount), AVG(price) FROM test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Aggregate functions were allowed");
    }

    @Test
    public void testSelectWithGroupByAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with GROUP BY should be allowed ---");
        String sql = "SELECT category, COUNT(*) FROM test_table GROUP BY category";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ GROUP BY was allowed");
    }

    @Test
    public void testSelectWithHavingAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with GROUP BY and HAVING should be allowed ---");
        String sql = "SELECT category, COUNT(*) as cnt FROM test_table GROUP BY category HAVING COUNT(*) > 5";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ GROUP BY with HAVING was allowed");
    }

    @Test
    public void testSelectWithWindowFunctionsAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with window functions should be allowed ---");
        String sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY name) as row_num FROM test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Window functions were allowed");
    }

    @Test
    public void testSelectWithCTEAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with CTE should be allowed ---");
        String sql = "WITH cte AS (SELECT * FROM table1) SELECT * FROM cte";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ CTE was allowed");
    }

    @Test
    public void testSelectWithUnionAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with UNION should be allowed ---");
        String sql = "SELECT * FROM table1 UNION SELECT * FROM table2";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ UNION was allowed");
    }

    @Test
    public void testSelectWithUnionAllAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with UNION ALL should be allowed ---");
        String sql = "SELECT * FROM table1 UNION ALL SELECT * FROM table2";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ UNION ALL was allowed");
    }

    @Test
    public void testSelectWithDistinctAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with DISTINCT should be allowed ---");
        String sql = "SELECT DISTINCT category FROM test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ DISTINCT was allowed");
    }

    @Test
    public void testSelectWithCaseAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with CASE should be allowed ---");
        String sql = "SELECT id, CASE WHEN value > 100 THEN 'high' WHEN value > 50 THEN 'medium' ELSE 'low' END as level FROM test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ CASE was allowed");
    }

    @Test
    public void testSelectWithLimitAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT with LIMIT should be allowed ---");
        String sql = "SELECT * FROM test_table LIMIT 10";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ LIMIT was allowed");
    }

    @Test
    public void testSelectFromTableFunctionsAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: SELECT from table function should be allowed ---");
        String sql = "SELECT * FROM read_parquet('test.parquet')";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Table function was allowed");
    }

    @Test
    public void testMultipleSelectsAllowed() throws SQLException, JsonProcessingException, UnauthorizedException {
        System.out.println("--- Test: Multiple SELECTs should be allowed ---");
        String sql = "SELECT 1; SELECT 2;";
        JsonNode queryTree = Transformations.parseToTree(sql);
        JsonNode result = SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        assertNotNull(result);
        System.out.println("✓ Multiple SELECTs were allowed");
    }

    // ===== Unauthorized Operation Tests =====

    @Test
    public void testInsertUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: INSERT should be unauthorized ---");
        String sql = "INSERT INTO test_table VALUES (1, 'test')";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ INSERT was unauthorized as expected");
    }

    @Test
    public void testUpdateUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: UPDATE should be unauthorized ---");
        String sql = "UPDATE test_table SET name = 'updated' WHERE id = 1";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ UPDATE was unauthorized as expected");
    }

    @Test
    public void testDeleteUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: DELETE should be unauthorized ---");
        String sql = "DELETE FROM test_table WHERE id = 1";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ DELETE was unauthorized as expected");
    }

    @Test
    public void testCreateTableUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: CREATE TABLE should be unauthorized ---");
        String sql = "CREATE TABLE new_table (id INT)";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ CREATE TABLE was unauthorized as expected");
    }

    @Test
    public void testDropTableUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: DROP TABLE should be unauthorized ---");
        String sql = "DROP TABLE test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ DROP TABLE was unauthorized as expected");
    }

    @Test
    public void testAlterTableUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: ALTER TABLE should be unauthorized ---");
        String sql = "ALTER TABLE test_table ADD COLUMN new_col VARCHAR";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ ALTER TABLE was unauthorized as expected");
    }

    @Test
    public void testTruncateUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: TRUNCATE TABLE should be unauthorized ---");
        String sql = "TRUNCATE TABLE test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ TRUNCATE was unauthorized as expected");
    }

    @Test
    public void testSelectAndInsertUnauthorized() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: SELECT followed by INSERT should be unauthorized ---");
        String sql = "SELECT 1; INSERT INTO test_table VALUES (1, 'test')";
        JsonNode queryTree = Transformations.parseToTree(sql);
        
        assertThrows(UnauthorizedException.class, () -> {
            SelectOnlyAuthorizer.INSTANCE.authorize("test-user", "test-db", "test-schema", queryTree, Map.of());
        });
        System.out.println("✓ SELECT; INSERT; was unauthorized as expected");
    }

    // ===== Parsing Error Tests (DML/DDL operations) =====
    // These operations are parsed by DuckDB but return error nodes

    @Test
    public void testInsertReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: INSERT should return error node ---");
        String sql = "INSERT INTO test_table VALUES (1, 'test')";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "INSERT should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ INSERT returned error node as expected");
    }

    @Test
    public void testUpdateReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: UPDATE should return error node ---");
        String sql = "UPDATE test_table SET name = 'updated' WHERE id = 1";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "UPDATE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ UPDATE returned error node as expected");
    }

    @Test
    public void testDeleteReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: DELETE should return error node ---");
        String sql = "DELETE FROM test_table WHERE id = 1";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "DELETE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ DELETE returned error node as expected");
    }

    @Test
    public void testCreateTableReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: CREATE TABLE should return error node ---");
        String sql = "CREATE TABLE new_table (id INT)";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "CREATE TABLE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ CREATE TABLE returned error node as expected");
    }

    @Test
    public void testDropTableReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: DROP TABLE should return error node ---");
        String sql = "DROP TABLE test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "DROP TABLE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ DROP TABLE returned error node as expected");
    }

    @Test
    public void testAlterTableReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: ALTER TABLE should return error node ---");
        String sql = "ALTER TABLE test_table ADD COLUMN new_col VARCHAR";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "ALTER TABLE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ ALTER TABLE returned error node as expected");
    }

    @Test
    public void testTruncateReturnsErrorNode() throws SQLException, JsonProcessingException {
        System.out.println("--- Test: TRUNCATE TABLE should return error node ---");
        String sql = "TRUNCATE TABLE test_table";
        JsonNode queryTree = Transformations.parseToTree(sql);
        assertNotNull(queryTree);
        assertTrue(queryTree.has("error"), "TRUNCATE should return error node");
        assertEquals("not implemented", queryTree.get("error_type").asText());
        System.out.println("✓ TRUNCATE returned error node as expected");
    }

    // ===== hasWriteAccess Test =====

    @Test
    public void testHasWriteAccessDenied() {
        System.out.println("--- Test: hasWriteAccess should return false ---");
        boolean result = SelectOnlyAuthorizer.INSTANCE.hasWriteAccess("test-user", "test-queue", Map.of());
        assertFalse(result);
        System.out.println("✓ Write access was denied");
    }
}
