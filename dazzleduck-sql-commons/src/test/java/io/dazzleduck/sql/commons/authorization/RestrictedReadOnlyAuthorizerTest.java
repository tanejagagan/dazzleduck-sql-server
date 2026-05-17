package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RestrictedReadOnlyAuthorizerTest {

    private static DuckDBConnection conn;
    private static final SqlAuthorizer authorizer = RestrictedReadOnlyAuthorizer.INSTANCE;

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE products (id INT, tenant_id VARCHAR, price INT)");
        conn.createStatement().execute("INSERT INTO products VALUES (1,'abc',10),(2,'xyz',20),(3,'abc',30)");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS products");
        conn.close();
    }

    private List<Object> execFirstColumn(String sql) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery(sql);
        List<Object> rows = new ArrayList<>();
        while (rs.next()) rows.add(rs.getObject(1));
        return rows;
    }

    // --- claim requirements ---

    @Test
    void noFilterClaim_throws() throws SQLException, JsonProcessingException {
        JsonNode tree = Transformations.parseToTree(conn, "SELECT * FROM products");
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "db", "main", tree, Map.of()));
    }

    @Test
    void blankFilterClaim_throws() throws SQLException, JsonProcessingException {
        JsonNode tree = Transformations.parseToTree(conn, "SELECT * FROM products");
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "db", "main", tree,
                        Map.of(Headers.HEADER_FILTER, "  ", Headers.HEADER_TABLE, "products")));
    }

    @Test
    void filterWithoutTable_throws() throws SQLException, JsonProcessingException {
        // 'filter' alone is not enough — must be scoped to a table via 'table' claim
        JsonNode tree = Transformations.parseToTree(conn, "SELECT id FROM products");
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "db", "main", tree,
                        Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'")));
    }

    // --- valid SELECT with filter + table ---

    @Test
    void filterWithTable_appliedToQuery() throws Exception {
        JsonNode tree = Transformations.parseToTree(conn, "SELECT id FROM products");
        JsonNode result = authorizer.authorize("user", "db", "main", tree,
                Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'",
                       Headers.HEADER_TABLE, "products"));
        String sql = Transformations.parseToSql(conn, result);

        List<Object> ids = execFirstColumn(sql);
        assertEquals(List.of(1, 3), ids);
    }

    /**
     * When the JWT carries DATABASE + SCHEMA claims, the legacy filter+table path must build
     * a fully-qualified key so that a same-named table in another schema is NOT covered by the
     * filter — it gets the deny-all {@code false} from injectFilterCtes.
     */
    @Test
    void filterWithQualifiedTable_sameNameInOtherSchema_isBlocked() throws Exception {
        conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS rro_other");
        conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS rro_other.products (id INT, tenant_id VARCHAR, price INT)");
        conn.createStatement().execute(
                "INSERT INTO rro_other.products VALUES (100,'abc',999),(101,'abc',888)");
        try {
            // JWT authorizes memory.main.products only; query references rro_other.products.
            JsonNode tree = Transformations.parseToTree(conn, "SELECT id FROM rro_other.products");
            JsonNode result = authorizer.authorize("user", "memory", "main", tree,
                    Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'",
                           Headers.HEADER_TABLE, "products",
                           Headers.HEADER_DATABASE, "memory",
                           Headers.HEADER_SCHEMA, "main"));
            String sql = Transformations.parseToSql(conn, result);
            List<Object> ids = execFirstColumn(sql);
            assertTrue(ids.isEmpty(),
                    "rro_other.products must receive false filter (not the memory.main.products filter). Got: " + ids);
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS rro_other.products");
            conn.createStatement().execute("DROP SCHEMA IF EXISTS rro_other");
        }
    }

    /**
     * Counter-test: when the JWT carries DATABASE + SCHEMA, an unqualified reference to the
     * authorized table still resolves via the suffix-alias in the normalized map.
     */
    @Test
    void filterWithQualifiedTable_bareReference_stillMatches() throws Exception {
        JsonNode tree = Transformations.parseToTree(conn, "SELECT id FROM products");
        JsonNode result = authorizer.authorize("user", "memory", "main", tree,
                Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'",
                       Headers.HEADER_TABLE, "products",
                       Headers.HEADER_DATABASE, "memory",
                       Headers.HEADER_SCHEMA, "main"));
        String sql = Transformations.parseToSql(conn, result);
        List<Object> ids = execFirstColumn(sql);
        assertEquals(List.of(1, 3), ids);
    }

    // --- write access always denied ---

    @Test
    void hasWriteAccess_alwaysFalse() {
        assertFalse(authorizer.hasWriteAccess("user", "queue", Map.of(
                Headers.HEADER_ACCESS_TYPE, "WRITE",
                Headers.QUERY_PARAMETER_INGESTION_QUEUE, "queue")));
    }

    // --- non-SELECT queries rejected ---

    @Test
    void insertQuery_rejected() throws SQLException, JsonProcessingException {
        // DuckDB's json_serialize_sql rejects non-SELECT; parse error surfaces as error node.
        // SelectOnlyAuthorizer blocks before the filter/table check.
        String sql = "INSERT INTO products VALUES (99, 'abc', 5)";
        try {
            JsonNode tree = Transformations.parseToTree(conn, sql);
            assertThrows(UnauthorizedException.class, () ->
                    authorizer.authorize("user", "db", "main", tree,
                            Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'",
                                   Headers.HEADER_TABLE, "products")));
        } catch (Exception e) {
            assertTrue(e.getMessage() != null);
        }
    }

    // --- multi-table join requires access ---

    @Test
    void join_tableAccess_filterAppliedToBothArms() throws Exception {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS reviews (product_id INT, tenant_id VARCHAR, rating INT)");
        conn.createStatement().execute("INSERT INTO reviews VALUES (1,'abc',5),(2,'xyz',3),(3,'abc',4)");
        try {
            String sql = "SELECT p.id FROM products p JOIN reviews r ON p.id = r.product_id";
            JsonNode tree = Transformations.parseToTree(conn, sql);

            // Multi-table query: must use access, not single filter
            String tableAccess = "[[\"table\",\"products\",\"*\",\"tenant_id = 'abc'\"],[\"table\",\"reviews\",\"*\",\"tenant_id = 'abc'\"]]";
            JsonNode result = authorizer.authorize("user", "db", "main", tree,
                    Map.of(Headers.HEADER_ACCESS, tableAccess));
            String out = Transformations.parseToSql(conn, result);
            List<Object> ids = execFirstColumn(out);
            assertEquals(2, ids.size());
            assertTrue(ids.contains(1));
            assertTrue(ids.contains(3));
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS reviews");
        }
    }

    // ── SECURITY: previously-exploitable inputs must now be rejected ──────────

    /** Multi-statement queries must be rejected at SelectOnlyAuthorizer. */
    @Test
    void multiStatement_rejected() throws Exception {
        String malicious = "SELECT id FROM products WHERE 1=1; SELECT id FROM products";
        JsonNode tree = Transformations.parseToTree(conn, malicious);
        String tableAccess = "[[\"table\",\"products\",\"*\",\"tenant_id = 'abc'\"]]";
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "memory", "main", tree,
                        Map.of(Headers.HEADER_ACCESS, tableAccess)));
    }

    /** TABLE_FUNCTION references (read_parquet, duckdb_tables, etc.) must be rejected. */
    @Test
    void tableFunction_rejected() throws Exception {
        JsonNode tree = Transformations.parseToTree(conn,
                "SELECT table_name FROM duckdb_tables() WHERE table_name = 'products'");
        String tableAccess = "[[\"table\",\"products\",\"*\",\"tenant_id = 'abc'\"]]";
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "memory", "main", tree,
                        Map.of(Headers.HEADER_ACCESS, tableAccess)));
    }

    /** External table functions like read_parquet must also be rejected. */
    @Test
    void tableFunction_readParquet_rejected() throws Exception {
        JsonNode tree = Transformations.parseToTree(conn,
                "SELECT * FROM read_parquet('/tmp/x.parquet')");
        String tableAccess = "[[\"table\",\"products\",\"*\",\"tenant_id = 'abc'\"]]";
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", "memory", "main", tree,
                        Map.of(Headers.HEADER_ACCESS, tableAccess)));
    }

    @Test
    void join_filterWithoutTableAccess_rejectsSecondTable() throws Exception {
        // filter+table only scopes to 'products'; 'reviews' gets deny-all → JOIN returns nothing
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS reviews2 (product_id INT, tenant_id VARCHAR, rating INT)");
        conn.createStatement().execute("INSERT INTO reviews2 VALUES (1,'abc',5),(3,'abc',4)");
        try {
            String sql = "SELECT p.id FROM products p JOIN reviews2 r ON p.id = r.product_id";
            JsonNode tree = Transformations.parseToTree(conn, sql);
            JsonNode result = authorizer.authorize("user", "db", "main", tree,
                    Map.of(Headers.HEADER_FILTER, "tenant_id = 'abc'",
                           Headers.HEADER_TABLE, "products"));
            String out = Transformations.parseToSql(conn, result);
            List<Object> ids = execFirstColumn(out);
            // reviews2 gets deny-all filter → no rows survive the join
            assertEquals(0, ids.size());
        } finally {
            conn.createStatement().execute("DROP TABLE IF EXISTS reviews2");
        }
    }
}
