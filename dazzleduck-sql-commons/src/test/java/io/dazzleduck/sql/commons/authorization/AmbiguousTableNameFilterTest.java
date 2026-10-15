package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Two granted tables can share a name in different schemas, and a row filter must then follow the
 * table a reference actually resolves to — never whichever grant the filter map happens to list
 * first. Here {@code memory.main.orders} is restricted to one tenant (400 of its 600) while
 * {@code memory.other.orders} is granted unrestricted, so applying the wrong grant to a bare
 * {@code orders} returns another tenant's rows.
 *
 * <p>Both multi-table authorizers share this lookup: redirect mode builds the map from the resolve
 * endpoint's rows in collector order, RESTRICT_READ_ONLY from the {@code access} claim's entries.
 * The map is exercised directly — which is redirect mode's code path — and through the
 * RESTRICT_READ_ONLY authorizer, with the grants in both orders each time.
 */
public class AmbiguousTableNameFilterTest {

    private static DuckDBConnection conn;
    private static final String DB = "memory";
    private static final String SCHEMA = "main";

    @BeforeAll
    static void setup() throws SQLException {
        conn = ConnectionPool.getConnection();
        var statement = conn.createStatement();
        statement.execute("CREATE SCHEMA IF NOT EXISTS other");
        statement.execute("CREATE TABLE main.orders (id INT, tenant_id VARCHAR, amount INT)");
        statement.execute("INSERT INTO main.orders VALUES (1,'abc',100),(2,'xyz',200),(3,'abc',300)");
        statement.execute("CREATE TABLE other.orders (id INT, tenant_id VARCHAR, amount INT)");
        statement.execute("INSERT INTO other.orders VALUES (1,'zzz',1),(2,'zzz',2),(3,'zzz',4)");
    }

    @AfterAll
    static void tearDown() throws SQLException {
        var statement = conn.createStatement();
        statement.execute("DROP TABLE IF EXISTS main.orders");
        statement.execute("DROP TABLE IF EXISTS other.orders");
        conn.close();
    }

    /** main.orders restricted to tenant abc; other.orders granted with no row restriction. */
    private static Map<String, JsonNode> grants(boolean otherFirst) {
        Map<String, JsonNode> grants = new LinkedHashMap<>();
        if (otherFirst) {
            grants.put("memory.other.orders", ExpressionFactory.trueExpression());
            grants.put("memory.main.orders", SqlAuthorizer.compileFilterString("tenant_id='abc'"));
        } else {
            grants.put("memory.main.orders", SqlAuthorizer.compileFilterString("tenant_id='abc'"));
            grants.put("memory.other.orders", ExpressionFactory.trueExpression());
        }
        return grants;
    }

    private static String runWithMap(String sql, boolean otherFirst) throws Exception {
        JsonNode tree = Transformations.parseToTree(conn, sql);
        JsonNode authorized = SqlAuthorizer.addFilterToEveryRead(
                SqlAuthorizer.withUpdatedDatabaseSchema(tree, DB, SCHEMA), grants(otherFirst), DB, SCHEMA);
        return firstValue(Transformations.parseToSql(conn, authorized));
    }

    private static String runAsReadOnly(String sql, boolean otherFirst) throws Exception {
        String restricted = "[\"table\",\"orders\",\"*\",\"tenant_id='abc'\"]";
        String unrestricted = "[\"table\",\"other.orders\",\"*\",\"true\"]";
        String access = otherFirst
                ? "[" + unrestricted + "," + restricted + "]"
                : "[" + restricted + "," + unrestricted + "]";
        JsonNode authorized = SqlAuthorizer.RESTRICT_READ_ONLY_AUTHORIZER.authorize(
                "user", DB, SCHEMA, Transformations.parseToTree(conn, sql),
                Map.of(Headers.HEADER_ACCESS, access));
        return firstValue(Transformations.parseToSql(conn, authorized));
    }

    private static String firstValue(String sql) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
            return rs.next() ? String.valueOf(rs.getObject(1)) : null;
        }
    }

    /** Each sums main.orders' amounts: 400 filtered, 600 if other.orders' grant is applied to it. */
    private static List<String> queriesSummingTheRestrictedTable() {
        return List.of(
                "SELECT sum(o1.amount) FROM other.orders o2 JOIN orders o1 ON o1.id = o2.id",
                "SELECT sum(o1.amount) FROM orders o1 JOIN other.orders o2 ON o1.id = o2.id",
                "SELECT (SELECT sum(amount) FROM orders) FROM other.orders LIMIT 1",
                "WITH o AS (SELECT amount FROM orders) SELECT sum(amount) FROM o");
    }

    @Test
    void aBareReferenceTakesItsOwnSchemasFilterWhicheverOrderTheGrantsAreIn() throws Exception {
        for (boolean otherFirst : new boolean[]{false, true}) {
            for (String sql : queriesSummingTheRestrictedTable()) {
                assertEquals("400", runWithMap(sql, otherFirst), "map, otherFirst=" + otherFirst + ": " + sql);
                assertEquals("400", runAsReadOnly(sql, otherFirst), "claim, otherFirst=" + otherFirst + ": " + sql);
            }
        }
    }

    /** The qualified reference keeps its own grant as well, rather than the bare name's filter. */
    @Test
    void aQualifiedReferenceKeepsItsOwnFilter() throws Exception {
        for (boolean otherFirst : new boolean[]{false, true}) {
            String sql = "SELECT sum(amount) FROM other.orders";
            assertEquals("7", runWithMap(sql, otherFirst), "map, otherFirst=" + otherFirst);
            assertEquals("7", runAsReadOnly(sql, otherFirst), "claim, otherFirst=" + otherFirst);
        }
    }

    /** A same-named table in a schema the grants do not cover still gets the deny-all filter. */
    @Test
    void aSameNamedTableInAnUngrantedSchemaGetsNoRows() throws Exception {
        Map<String, JsonNode> onlyMain = Map.of(
                "memory.main.orders", SqlAuthorizer.compileFilterString("tenant_id='abc'"));
        JsonNode tree = Transformations.parseToTree(conn, "SELECT sum(amount) FROM other.orders");
        JsonNode authorized = SqlAuthorizer.addFilterToEveryRead(
                SqlAuthorizer.withUpdatedDatabaseSchema(tree, DB, SCHEMA), onlyMain, DB, SCHEMA);
        assertEquals("null", firstValue(Transformations.parseToSql(conn, authorized)));
    }

    /** Two keys naming one table with different filters is a contradictory grant, not a race. */
    @Test
    void grantsThatResolveToOneTableWithDifferentFiltersAreRejected() throws Exception {
        Map<String, JsonNode> contradictory = new LinkedHashMap<>();
        contradictory.put("orders", SqlAuthorizer.compileFilterString("tenant_id='abc'"));
        contradictory.put("memory.main.orders", ExpressionFactory.trueExpression());
        JsonNode tree = Transformations.parseToTree(conn, "SELECT sum(amount) FROM orders");
        assertThrows(IllegalArgumentException.class,
                () -> SqlAuthorizer.addFilterToEveryRead(tree, contradictory, DB, SCHEMA));
    }
}
