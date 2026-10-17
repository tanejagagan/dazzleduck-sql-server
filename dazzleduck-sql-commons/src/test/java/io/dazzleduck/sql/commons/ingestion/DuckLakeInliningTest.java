package io.dazzleduck.sql.commons.ingestion;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link DuckLakeInlining} resolves the limit that actually applies to a table, over five sources
 * that are not all visible in the same view. One test per source, because a resolver that misses
 * one is wrong in both directions: it refuses a table that is fine, or passes a table that inlines.
 *
 * <p>The behaviour each source produces — which of them beat which — was established against
 * DuckLake directly; what is pinned here is that this code reads them and resolves them in
 * DuckLake's order.
 */
public class DuckLakeInliningTest {

    @TempDir
    Path dir;

    private String catalog;
    private String settingBefore;

    /**
     * {@link DuckLakeInlining#SETTING} is process-wide, and these tests move it. Surefire runs the
     * whole module in one JVM against one DuckDB instance, so leaving it changed alters how many
     * files OTHER tests' inserts produce — which it did, before this was restored.
     */
    @BeforeEach
    void rememberSetting() throws Exception {
        settingBefore = ConnectionPool.collectFirst(
                "SELECT current_setting('%s')::VARCHAR".formatted(DuckLakeInlining.SETTING), String.class);
    }

    @AfterEach
    void detachAndRestoreSetting() {
        if (catalog != null) {
            ConnectionPool.execute("DETACH " + catalog);
            catalog = null;
        }
        if (settingBefore != null) {
            ConnectionPool.execute("SET GLOBAL %s = %s".formatted(DuckLakeInlining.SETTING, settingBefore));
        }
    }

    /** Attaches a catalog, optionally with the inlining parameter on the ATTACH itself. */
    private String attach(String name, Integer attachLimit) throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = name;
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s'%s)".formatted(
                dir.resolve("catalog"), name, dir.resolve("data"),
                attachLimit == null ? "" : ", DATA_INLINING_ROW_LIMIT " + attachLimit));
        ConnectionPool.execute("CREATE SCHEMA IF NOT EXISTS %s.other".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.main.t (a INT)".formatted(name));
        ConnectionPool.execute("CREATE TABLE %s.other.t (a INT)".formatted(name));
        return name;
    }

    /**
     * Writes the option row that {@code CALL c.set_option(...)} produces. Written directly because
     * that call is not reachable through the JDBC driver in this DuckLake build, though it works in
     * the CLI; the stored form was read back from a catalog the CLI had configured.
     *
     * @param scope      {@code table}, {@code schema} or {@code global}
     * @param scopeEntry for {@code table}, {@code schema.table}; for {@code schema}, its name
     */
    private void setOption(String scope, String scopeEntry, int limit) {
        String scopeId = switch (scope) {
            // Qualified, because two schemas here hold a table of the same name.
            case "table" -> ("(SELECT t.table_id FROM __ducklake_metadata_%s.ducklake_table t"
                    + " JOIN __ducklake_metadata_%s.ducklake_schema s ON (s.schema_id = t.schema_id)"
                    + " WHERE s.schema_name = '%s' AND t.table_name = '%s'"
                    + " AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL)");
            case "schema" -> "(SELECT schema_id FROM __ducklake_metadata_%s.ducklake_schema"
                    + " WHERE schema_name = '%s' AND end_snapshot IS NULL)";
            default -> "NULL";
        };
        ConnectionPool.execute(("INSERT INTO __ducklake_metadata_%s.ducklake_metadata"
                + " (key, value, scope, scope_id) VALUES ('%s', '%d', %s, %s)").formatted(
                        catalog, DuckLakeInlining.OPTION, limit,
                        scope.equals("global") ? "NULL" : "'" + scope + "'",
                        switch (scope) {
                            case "table" -> scopeId.formatted(catalog, catalog,
                                    scopeEntry.substring(0, scopeEntry.indexOf('.')),
                                    scopeEntry.substring(scopeEntry.indexOf('.') + 1));
                            case "schema" -> scopeId.formatted(catalog, scopeEntry);
                            default -> scopeId;
                        }));
    }

    @Test
    void withNothingSetTheInstanceSettingApplies() throws Exception {
        attach("inlining_setting_test", null);
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(7L, limit.rowLimit());
        assertEquals(DuckLakeInlining.SETTING, limit.source());
        assertTrue(limit.inliningEnabled());
    }

    /**
     * Instance-wide, which is the only scope this setting really has here: a per-connection
     * {@code SET} on a {@link ConnectionPool} duplicate moved it for the others too.
     */
    @Test
    void disableZeroesTheSettingForTheInstance() throws Exception {
        attach("inlining_disable_test", null);
        ConnectionPool.execute("SET GLOBAL %s = 7".formatted(DuckLakeInlining.SETTING));
        DuckLakeInlining.disable();
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(0L, limit.rowLimit());
        assertFalse(limit.inliningEnabled());
    }

    /**
     * The source a resolver is most likely to miss: it is in {@code duckdb_databases().options} and
     * in neither {@code ducklake_options()} nor {@code duckdb_settings()}, and it beats the setting
     * — so zeroing the setting is not sufficient.
     */
    @Test
    void theAttachParameterBeatsTheInstanceSetting() throws Exception {
        attach("inlining_attach_test", 10);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(10L, limit.rowLimit(), "the ATTACH parameter still applies");
        assertTrue(limit.source().contains("ATTACH"), limit.source());
        assertEquals(0L, (long) ConnectionPool.collectFirst(
                "SELECT count(*) FROM ducklake_options('%s') WHERE option_name = '%s'"
                        .formatted(catalog, DuckLakeInlining.OPTION), Long.class),
                "and it is invisible to ducklake_options, which is what makes it the trap");
    }

    @Test
    void aCatalogOptionBeatsTheAttachParameter() throws Exception {
        attach("inlining_catalog_test", 10);
        setOption("global", null, 3);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(3L, limit.rowLimit());
        assertTrue(limit.source().contains("catalog"), limit.source());
    }

    /** The fifth source, which sits between table and catalog. */
    @Test
    void aSchemaOptionBeatsTheCatalogAndAppliesOnlyToThatSchema() throws Exception {
        attach("inlining_schema_test", 10);
        setOption("global", null, 3);
        setOption("schema", "other", 0);
        var other = DuckLakeInlining.effectiveRowLimit(catalog, "other", "t");
        assertEquals(0L, other.rowLimit(), "the schema's own zero");
        assertTrue(other.source().contains("schema 'other'"), other.source());
        assertEquals(3L, DuckLakeInlining.effectiveRowLimit(catalog, "main", "t").rowLimit(),
                "and the other schema keeps the catalog's value");
    }

    @Test
    void aTableOptionBeatsEverythingElse() throws Exception {
        attach("inlining_table_test", 10);
        setOption("global", null, 3);
        setOption("schema", "main", 5);
        setOption("table", "main.t", 0);
        var limit = DuckLakeInlining.effectiveRowLimit(catalog, "main", "t");
        assertEquals(0L, limit.rowLimit());
        assertTrue(limit.source().contains("table"), limit.source());
    }

    /** A value that is not a number means the real limit is unknown, not that it is unset. */
    @Test
    void aNonNumericAttachValueIsReportedRatherThanIgnored() throws Exception {
        Files.createDirectories(dir.resolve("data"));
        catalog = "inlining_bad_value_test";
        // DuckLake accepts the ATTACH; what it stores is what the resolver has to cope with.
        try {
            ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 'lots')"
                    .formatted(dir.resolve("catalog"), catalog, dir.resolve("data")));
        } catch (RuntimeException e) {
            catalog = null;
            return; // DuckLake rejected it at ATTACH time, which is just as good
        }
        ConnectionPool.execute("CREATE TABLE %s.main.t (a INT)".formatted(catalog));
        assertThrows(java.sql.SQLException.class,
                () -> DuckLakeInlining.effectiveRowLimit(catalog, "main", "t"));
    }

    /** The remedy names the source's own fix first, since it is the narrower change. */
    @Test
    void theRemedyNamesTheSourcesOwnFixBeforeTheStoredOption() {
        String forAttach = DuckLakeInlining.remedy("c", "main", "t", "the ATTACH parameter DATA_INLINING_ROW_LIMIT");
        assertTrue(forAttach.indexOf("remove DATA_INLINING_ROW_LIMIT")
                < forAttach.indexOf("set_option"), forAttach);
        assertTrue(DuckLakeInlining.remedy("c", "main", "t", "the catalog's data_inlining_row_limit")
                .contains("set it to 0 where it is set"));
    }
}
