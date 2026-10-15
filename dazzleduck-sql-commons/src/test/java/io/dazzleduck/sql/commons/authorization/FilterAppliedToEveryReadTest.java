package io.dazzleduck.sql.commons.authorization;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * RESTRICTED mode's row filter must apply to EVERY read of the authorized table, not only the
 * outermost SELECT's WHERE: a table read in a scalar subquery, an EXISTS, or a CTE body is the same
 * table and must return the same rows. Each query below reads only the authorized table, so it
 * passes the access check; what is tested is which rows come back.
 */
public class FilterAppliedToEveryReadTest {

    private static DuckDBConnection conn;
    private static final SqlAuthorizer authorizer = SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER;
    private static final String DB = "memory";
    private static final String SCHEMA = "main";

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        conn.createStatement().execute("CREATE TABLE orders (id INT, tenant_id VARCHAR, amount INT)");
        conn.createStatement().execute("INSERT INTO orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS orders");
        conn.close();
    }

    /** Both claim shapes grant the same thing and must behave identically. */
    private static List<Map<String, String>> grants() {
        return List.of(
                Map.of(Headers.HEADER_TABLE, "orders", Headers.HEADER_FILTER, "tenant_id='abc'"),
                Map.of(Headers.HEADER_ACCESS, "[[\"table\",\"orders\",\"*\",\"tenant_id='abc'\"]]"));
    }

    private static List<Object> run(String sql, Map<String, String> claims) throws Exception {
        JsonNode authorized = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims);
        String rewritten = Transformations.parseToSql(conn, authorized);
        return firstColumn(rewritten);
    }

    /** First column of every row, as text, so a wider numeric type than expected cannot matter. */
    private static List<Object> firstColumn(String sql) throws SQLException {
        List<Object> rows = new ArrayList<>();
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            while (rs.next()) rows.add(String.valueOf(rs.getObject(1)));
        }
        return rows;
    }

    @Test
    void theTopLevelReadIsFiltered() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("1", "3"), run("SELECT id FROM orders ORDER BY id", claims));
        }
    }

    @Test
    void aScalarSubqueryReadIsFiltered() throws Exception {
        // Unfiltered this is 600 (every tenant); filtered it is 400 (tenant abc only).
        for (var claims : grants()) {
            assertEquals(List.of("400", "400"),
                    run("SELECT (SELECT sum(amount) FROM orders) AS total FROM orders", claims));
        }
    }

    @Test
    void aSubqueryCannotLeakAnotherTenantsColumnValues() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("abc:100,abc:300", "abc:100,abc:300"),
                    run("SELECT (SELECT string_agg(tenant_id || ':' || amount, ',' ORDER BY amount) FROM orders) AS v FROM orders", claims));
        }
    }

    @Test
    void anExistsSubqueryCannotTestAnotherTenantsRows() throws Exception {
        // The EXISTS looks for a tenant 'xyz' row; filtered, it can never find one, so no rows.
        for (var claims : grants()) {
            assertEquals(List.of(),
                    run("SELECT id FROM orders WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.tenant_id = 'xyz')", claims));
        }
    }

    @Test
    void aCteBodyReadIsFiltered() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("400"), run("WITH x AS (SELECT * FROM orders) SELECT sum(amount) FROM x", claims));
        }
    }

    @Test
    void aJoinOfTheTableToItselfIsFilteredOnBothSides() throws Exception {
        for (var claims : grants()) {
            assertEquals(List.of("2"), run("SELECT count(*) FROM orders a JOIN orders b ON a.id = b.id", claims));
        }
    }

    /**
     * The Flight server authorizes a query once for its info and once for its stream, so the second
     * pass sees a query that already carries filter CTEs. It must stay valid and filtered.
     */
    @Test
    void authorizingAnAlreadyAuthorizedQueryStaysValidAndFiltered() throws Exception {
        for (var claims : grants()) {
            for (String sql : new String[]{"SELECT id FROM orders ORDER BY id", "SELECT id FROM memory.main.orders ORDER BY id"}) {
                JsonNode once = authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims);
                JsonNode twice = authorizer.authorize("user", DB, SCHEMA, once, claims);
                assertEquals(List.of("1", "3"), firstColumn(Transformations.parseToSql(conn, twice)), sql);
            }
        }
    }

    /**
     * A filtered path grant whose filter has nowhere to go is refused. The old code returned
     * {@code null} from the filter step, which the caller dereferenced.
     */
    @Test
    void aFilteredFunctionGrantRefusesAReadItCannotFilter() {
        String sql = "SELECT 1 AS c FROM (SELECT count(*) FROM read_parquet('example/data/hive_table/*/*/*.parquet')) u";
        var claims = Map.of(Headers.HEADER_ACCESS,
                "[[\"path\",\"example/data/hive_table\",\"*\",\"key='k2'\"]]");
        assertThrows(UnauthorizedException.class, () ->
                authorizer.authorize("user", DB, SCHEMA, Transformations.parseToTree(conn, sql), claims));
    }
}
