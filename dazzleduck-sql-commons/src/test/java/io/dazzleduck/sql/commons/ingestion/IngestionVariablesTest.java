package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-queue ingestion variables: what the config file declares, what a key/value relation supplies,
 * and which of the two wins.
 */
class IngestionVariablesTest {

    private static final String QUEUE = "logs";
    /** A plain (non-TEMP) table: {@code resolve} reads it on a connection of its own. */
    private static final String RELATION = "ing_vars";

    @BeforeEach
    void createRelation() throws Exception {
        ConnectionPool.execute("CREATE OR REPLACE TABLE " + RELATION + "(key VARCHAR, value VARCHAR)");
    }

    @AfterEach
    void dropRelation() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS " + RELATION);
    }

    private static void insert(String key, String value) throws Exception {
        ConnectionPool.execute("INSERT INTO %s VALUES ('%s', '%s')".formatted(RELATION, key, value));
    }

    private static IngestionVariables fromConfig(String hocon) {
        return IngestionVariables.fromConfig(ConfigFactory.parseString(hocon));
    }

    // -----------------------------------------------------------------------
    // Static variables, from the config file
    // -----------------------------------------------------------------------

    @Test
    void noVariablesConfiguredIsNone() {
        var variables = fromConfig("ingestion_queue = \"logs\"");
        assertSameAsNone(variables);
        assertTrue(variables.resolve(QUEUE).isEmpty());
    }

    @Test
    void staticVariablesAreReadOnceFromTheFile() {
        var variables = fromConfig("variables { env = \"prod\", tier = \"hot\" }");
        assertFalse(variables.hasView(), "no relation configured — resolve must not query");
        assertEquals(Map.of("env", "prod", "tier", "hot"), variables.resolve(QUEUE));
    }

    @Test
    void nonStringStaticValuesAreTakenAsStringsBecauseEveryVariableIsAVarchar() {
        var variables = fromConfig("variables { retention_days = 30, enabled = true }");
        assertEquals(Map.of("retention_days", "30", "enabled", "true"), variables.resolve(QUEUE));
    }

    @Test
    void aStaticNameThatCannotBeAVariableFailsFast() {
        // The config file is unambiguously this queue's own, so a bad name there is an error at
        // startup rather than something skipped silently (unlike a row of a shared relation).
        var ex = assertThrows(IllegalArgumentException.class,
                () -> fromConfig("variables { \"my var\" = \"x\" }"));
        assertTrue(ex.getMessage().contains("ingestion variable name"), ex.getMessage());
        assertThrows(IllegalArgumentException.class, () -> fromConfig("variables { nested { a = 1 } }"));
    }

    // -----------------------------------------------------------------------
    // Variables from a relation
    // -----------------------------------------------------------------------

    @Test
    void relationRowsBecomeVariables() throws Exception {
        insert("env", "prod");
        insert("tier", "hot");
        var variables = fromConfig("variables_view = \"" + RELATION + "\"");
        assertTrue(variables.hasView());
        assertEquals(Map.of("env", "prod", "tier", "hot"), variables.resolve(QUEUE));
    }

    @Test
    void relationOverridesTheFilesValue() throws Exception {
        insert("env", "staging");
        var variables = fromConfig("""
                variables { env = "prod", tier = "hot" }
                variables_view = "%s"
                """.formatted(RELATION));
        // The file holds the deployment's defaults; the relation is what an operator changes.
        assertEquals(Map.of("env", "staging", "tier", "hot"), variables.resolve(QUEUE));
    }

    @Test
    void columnNamesAreConfigurable() throws Exception {
        ConnectionPool.execute("CREATE OR REPLACE TABLE other_vars(config_key VARCHAR, v VARCHAR)");
        try {
            ConnectionPool.execute("INSERT INTO other_vars VALUES ('env', 'prod')");
            var variables = fromConfig("""
                    variables_view         = "other_vars"
                    variables_key_column   = "config_key"
                    variables_value_column = "v"
                    """);
            assertEquals(Map.of("env", "prod"), variables.resolve(QUEUE));
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS other_vars");
        }
    }

    @Test
    void aKeyThatCannotBeAVariableNameFailsNamingTheRelation() throws Exception {
        // The relation holds this queue's own variables, so such a key is a mistake in it — not
        // another consumer's row to step over. A deployment sharing one table gives each queue a
        // view that selects and renames its rows, and the view is what this config points at.
        insert("env", "prod");
        insert("compaction.snapshot_retention", "60 minutes");
        var variables = fromConfig("variables_view = \"" + RELATION + "\"");
        var ex = assertThrows(IllegalArgumentException.class, () -> variables.resolve(QUEUE));
        assertTrue(ex.getMessage().contains("compaction.snapshot_retention"), ex.getMessage());
        assertTrue(ex.getMessage().contains(RELATION), ex.getMessage());
        assertTrue(ex.getMessage().contains(QUEUE), ex.getMessage());
    }

    @Test
    void rowsWithNoNameOrNoValueAreIgnored() throws Exception {
        // Neither says anything: getvariable() of a name that was never set already reads NULL.
        insert("env", "prod");
        ConnectionPool.execute("INSERT INTO %s VALUES (NULL, 'x'), ('unset', NULL)".formatted(RELATION));
        var variables = fromConfig("variables_view = \"" + RELATION + "\"");
        assertEquals(Map.of("env", "prod"), variables.resolve(QUEUE));
    }

    @Test
    void aRowAddressedHereWithAnUnusableValueFailsTheLoad() throws Exception {
        // Writing the batch anyway would mean a transformation silently reading a truncated value.
        insert("env", "x".repeat(5000));
        var variables = fromConfig("variables_view = \"" + RELATION + "\"");
        var ex = assertThrows(IllegalArgumentException.class, () -> variables.resolve(QUEUE));
        assertTrue(ex.getMessage().contains("too long"), ex.getMessage());
    }

    @Test
    void anUnreadableRelationFailsNamingTheQueueAndTheRelation() {
        var variables = fromConfig("variables_view = \"no_such_relation\"");
        var ex = assertThrows(RuntimeException.class, () -> variables.resolve(QUEUE));
        assertTrue(ex.getMessage().contains(QUEUE), ex.getMessage());
        assertTrue(ex.getMessage().contains("no_such_relation"), ex.getMessage());
    }

    @Test
    void relationAndColumnNamesMustBePlainIdentifiers() {
        // They are interpolated into the SELECT, so SQL text in them is a config error.
        assertThrows(IllegalArgumentException.class,
                () -> fromConfig("variables_view = \"v_vars; DROP TABLE orders\""));
        assertThrows(IllegalArgumentException.class, () -> fromConfig("""
                variables_view       = "v_vars"
                variables_key_column = "key, (SELECT 1)"
                """));
    }

    // -----------------------------------------------------------------------
    // Expiry
    // -----------------------------------------------------------------------

    private static final String EXPIRING = "ing_vars_exp";

    /** A relation with an expiration column, holding one row per (name, value, expiry) given. */
    private static void createExpiringRelation(String columnType, String... rows) throws Exception {
        ConnectionPool.execute("CREATE OR REPLACE TABLE %s(key VARCHAR, value VARCHAR, expires_at %s)"
                .formatted(EXPIRING, columnType));
        for (String row : rows) {
            ConnectionPool.execute("INSERT INTO %s VALUES (%s)".formatted(EXPIRING, row));
        }
    }

    private static IngestionVariables expiringConfig() {
        return fromConfig("""
                variables_view              = "%s"
                variables_expiration_column = "expires_at"
                """.formatted(EXPIRING));
    }

    @Test
    void anExpiredRowIsNoLongerSet() throws Exception {
        createExpiringRelation("TIMESTAMPTZ",
                "'env', 'prod', now() - INTERVAL 1 HOUR",
                "'tier', 'hot', now() + INTERVAL 1 HOUR");
        // The expired name is absent entirely, so getvariable() reads NULL for it.
        assertEquals(Map.of("tier", "hot"), expiringConfig().resolve(QUEUE));
    }

    @Test
    void aRowWithoutAnExpirationNeverExpires() throws Exception {
        createExpiringRelation("TIMESTAMPTZ", "'env', 'prod', NULL");
        assertEquals(Map.of("env", "prod"), expiringConfig().resolve(QUEUE));
    }

    @Test
    void expiryIsEvaluatedForAPlainTimestampColumnToo() throws Exception {
        // DuckDB compares the column against its own now(), so a TIMESTAMP and a TIMESTAMPTZ are
        // each read the way that engine defines — this code never reinterprets a zone.
        createExpiringRelation("TIMESTAMP",
                "'gone', 'x', now()::TIMESTAMP - INTERVAL 1 DAY",
                "'live', 'y', now()::TIMESTAMP + INTERVAL 1 DAY");
        assertEquals(Map.of("live", "y"), expiringConfig().resolve(QUEUE));
    }

    @Test
    void anExpiredRowFallsBackToTheFilesValue() throws Exception {
        createExpiringRelation("TIMESTAMPTZ", "'env', 'staging', now() - INTERVAL 1 HOUR");
        var variables = fromConfig("""
                variables { env = "prod" }
                variables_view              = "%s"
                variables_expiration_column = "expires_at"
                """.formatted(EXPIRING));
        // The relation overrides the file only while its row is live.
        assertEquals(Map.of("env", "prod"), variables.resolve(QUEUE));
    }

    @Test
    void expiryIsOptedIntoByConfiguringTheColumn() throws Exception {
        // The column is only read when named, so a relation that has one but a config that does not
        // mention it keeps every row — and a relation without one keeps working unchanged.
        createExpiringRelation("TIMESTAMPTZ", "'env', 'prod', now() - INTERVAL 1 HOUR");
        var variables = fromConfig("variables_view = \"%s\"".formatted(EXPIRING));
        assertFalse(variables.view().hasExpiration());
        assertEquals(Map.of("env", "prod"), variables.resolve(QUEUE));
    }

    @Test
    void twoLiveRowsForOneNameFailRatherThanPickOne() throws Exception {
        // Which row wins would otherwise depend on scan order, which a view without an ORDER BY
        // does not pin down.
        insert("env", "prod");
        insert("env", "staging");
        var variables = fromConfig("variables_view = \"" + RELATION + "\"");
        var ex = assertThrows(IllegalArgumentException.class, () -> variables.resolve(QUEUE));
        assertTrue(ex.getMessage().contains("more than once"), ex.getMessage());
        assertTrue(ex.getMessage().contains("env"), ex.getMessage());
    }

    @Test
    void anExpiredRowAlongsideALiveOneIsHowAValueRotates() throws Exception {
        // The expired row is skipped before the duplicate check, so keeping history (or rotating a
        // value by expiring the old row) is not mistaken for an ambiguous definition.
        createExpiringRelation("TIMESTAMPTZ",
                "'token', 'old', now() - INTERVAL 1 HOUR",
                "'token', 'new', now() + INTERVAL 1 HOUR");
        assertEquals(Map.of("token", "new"), expiringConfig().resolve(QUEUE));
    }

    @Test
    void theExpirationColumnMustBeAPlainIdentifier() {
        assertThrows(IllegalArgumentException.class, () -> fromConfig("""
                variables_view              = "v_vars"
                variables_expiration_column = "expires_at, (SELECT 1)"
                """));
    }

    @AfterEach
    void dropExpiringRelation() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS " + EXPIRING);
    }

    // -----------------------------------------------------------------------
    // Connection scoping
    // -----------------------------------------------------------------------

    @Test
    void aVariableSetOnOneConnectionIsNotVisibleOnAnother() throws Exception {
        // What keeps one queue's variables out of another queue's write — and out of a user's
        // query — is that SET VARIABLE is scoped to the connection it ran on, and ConnectionPool
        // hands out a duplicate per use. Assert that rather than trusting it.
        try (var writer = ConnectionPool.getConnection();
             var statement = writer.createStatement()) {
            statement.execute("SET VARIABLE \"queue_local\" = 'queue-a'");
            try (var st = writer.createStatement();
                 var rs = st.executeQuery("SELECT getvariable('queue_local')")) {
                assertTrue(rs.next());
                assertEquals("queue-a", rs.getString(1), "visible to later statements on the same connection");
            }
            try (var other = ConnectionPool.getConnection();
                 var st = other.createStatement();
                 var rs = st.executeQuery("SELECT getvariable('queue_local')")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1), "must NOT leak to another connection");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Carried on the mapping
    // -----------------------------------------------------------------------

    @Test
    void aMappingCarriesItsVariablesAndComparesByValue() {
        var mapping = new QueueIdToTableMapping(QUEUE, "lake", "main", "logs", Map.of(), null);
        assertSameAsNone(mapping.variables());

        var withVariables = mapping.withVariables(fromConfig("variables { env = \"prod\" }"));
        assertEquals(Map.of("env", "prod"), withVariables.variables().resolve(QUEUE));
        // updateMappings() reconciles on equality, so an unchanged variable set must compare equal.
        assertEquals(withVariables, mapping.withVariables(fromConfig("variables { env = \"prod\" }")));
        assertFalse(withVariables.equals(mapping.withVariables(fromConfig("variables { env = \"dev\" }"))));
    }

    private static void assertSameAsNone(IngestionVariables variables) {
        assertTrue(variables.isEmpty());
        assertFalse(variables.hasView());
        assertEquals(IngestionVariables.NONE, variables);
    }
}
