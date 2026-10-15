package io.dazzleduck.sql.commons;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Which names a CTE body can see, for {@link Transformations#collectAllTableReferences}. As in
 * DuckDB, a non-recursive CTE body sees the enclosing scope and the CTEs declared before it, but
 * not itself, so a name it uses for itself resolves to the table of that name. The query that
 * follows the WITH clause sees every CTE. Tables are compared by distinct name.
 */
class CollectTableReferencesCteScopeTest {

    private static List<String> tables(String sql) throws Exception {
        return Transformations.collectAllTableReferences(
                        Transformations.getFirstStatementNode(Transformations.parseToTree(sql)), "", "")
                .stream().map(t -> t.tableOrPath()).distinct().sorted().toList();
    }

    @Test
    void aCteBodyUsingTheCtesOwnNameReadsTheTable() throws Exception {
        assertEquals(List.of("t"), tables("WITH t AS (SELECT * FROM t WHERE x > 0) SELECT * FROM t"));
    }

    @Test
    void aCteBodySeesTheCtesDeclaredBeforeIt() throws Exception {
        assertEquals(List.of("src"), tables("WITH a AS (SELECT * FROM src), b AS (SELECT * FROM a) SELECT * FROM b"));
    }

    @Test
    void aLaterCteUsingItsOwnNameReadsTheTable() throws Exception {
        assertEquals(List.of("b", "src"), tables("WITH a AS (SELECT * FROM src), b AS (SELECT * FROM b JOIN a USING (id)) SELECT * FROM b"));
    }

    // A forward reference — a CTE body naming a LATER sibling — binds to the real table, not the
    // CTE: verified in DuckDB 1.5.5, where `WITH a AS (SELECT * FROM b), b AS (SELECT * FROM src)
    // SELECT * FROM a` reads table b. So the table must be reported.

    @Test
    void aCteBodyNamingALaterCteReadsTheTable() throws Exception {
        // b: the forward reference in a's body. src: read by b's own body, reported as well, since
        // every CTE body is walked whether or not the main query uses it.
        assertEquals(List.of("b", "src"), tables("WITH a AS (SELECT * FROM b), b AS (SELECT * FROM src) SELECT * FROM a"));
    }

    @Test
    void aForwardReferenceIsReportedAlongsideTheTablesItJoins() throws Exception {
        assertEquals(List.of("later", "other", "src"), tables(
                "WITH first AS (SELECT * FROM src JOIN later USING (id)), later AS (SELECT * FROM other) "
                        + "SELECT * FROM first"));
    }

    @Test
    void aForwardReferenceInASubqueryReadsTheTable() throws Exception {
        assertEquals(List.of("later", "src"), tables(
                "WITH first AS (SELECT * FROM src WHERE id IN (SELECT id FROM later)), later AS (SELECT 1 AS id) "
                        + "SELECT * FROM first"));
    }

    @Test
    void theMainQuerySeesEveryCte() throws Exception {
        assertEquals(List.of("src"), tables("WITH a AS (SELECT * FROM src), b AS (SELECT 1 AS one) SELECT * FROM a, b"));
    }

    @Test
    void cteNamesInSubqueriesStillShadowTables() throws Exception {
        assertEquals(List.of("src"), tables("WITH t AS (SELECT * FROM src) SELECT * FROM t WHERE id IN (SELECT id FROM t)"));
    }

    @Test
    void aRecursiveCteStillSeesItself() throws Exception {
        assertEquals(List.of("edges"), tables(
                "WITH RECURSIVE r AS (SELECT src, dst FROM edges UNION ALL SELECT r.src, e.dst FROM r JOIN edges e ON r.dst = e.src) "
                        + "SELECT * FROM r"));
    }
}
