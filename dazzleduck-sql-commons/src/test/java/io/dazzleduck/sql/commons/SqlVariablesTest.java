package io.dazzleduck.sql.commons;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared {@code SET VARIABLE} rendering rules. The claim-specific behaviour on top of them
 * (JSON parsing, the quote hint for a bare number) is covered by
 * {@code authorization.SessionVariablesTest}; what runs on DuckDB is covered there too.
 */
class SqlVariablesTest {

    private static final String NOUN = "ingestion variable";

    @Test
    void rendersOneStatementPerEntryInIterationOrder() {
        var variables = new LinkedHashMap<String, String>();
        variables.put("env", "prod");
        variables.put("tier", "hot");
        assertEquals(List.of(
                "SET VARIABLE \"env\" = 'prod'",
                "SET VARIABLE \"tier\" = 'hot'"),
                SqlVariables.toSetStatements(variables, NOUN));
    }

    @Test
    void nullAndEmptyYieldNoStatements() {
        assertTrue(SqlVariables.toSetStatements(null, NOUN).isEmpty());
        assertTrue(SqlVariables.toSetStatements(Map.of(), NOUN).isEmpty());
        assertDoesNotThrow(() -> SqlVariables.validate(null, NOUN));
    }

    @Test
    void valueStaysInsideOneLiteral() {
        assertEquals(List.of("SET VARIABLE \"t\" = 'x''; DROP TABLE orders; --'"),
                SqlVariables.toSetStatements(Map.of("t", "x'; DROP TABLE orders; --"), NOUN));
    }

    @Test
    void reservedWordNameIsQuoted() {
        assertEquals(List.of("SET VARIABLE \"table\" = 'x'"),
                SqlVariables.toSetStatements(Map.of("table", "x"), NOUN));
    }

    @Test
    void invalidNameIsRejectedAndNamesTheSource() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SqlVariables.toSetStatements(Map.of("my var", "x"), NOUN));
        assertTrue(ex.getMessage().contains("Invalid ingestion variable name"), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> SqlVariables.toSetStatements(Map.of("1st", "x"), NOUN));
        assertThrows(IllegalArgumentException.class,
                () -> SqlVariables.toSetStatements(Map.of("a.b", "x"), NOUN));
    }

    @Test
    void isValidNameTellsAForeignKeyFromAVariableName() {
        assertTrue(SqlVariables.isValidName("tenant_id"));
        assertFalse(SqlVariables.isValidName("compaction.snapshot_retention"));
        assertFalse(SqlVariables.isValidName(null));
        assertFalse(SqlVariables.isValidName(""));
    }

    @Test
    void oversizedAndMalformedValuesAreRejected() {
        var tooLong = assertThrows(IllegalArgumentException.class, () -> SqlVariables.toSetStatements(
                Map.of("a", "x".repeat(SqlVariables.MAX_VALUE_LENGTH + 1)), NOUN));
        assertTrue(tooLong.getMessage().contains("too long"), tooLong.getMessage());

        var control = assertThrows(IllegalArgumentException.class,
                () -> SqlVariables.toSetStatements(Map.of("a", "x" + (char) 0 + "y"), NOUN));
        assertTrue(control.getMessage().contains("control character"), control.getMessage());

        var tooMany = new LinkedHashMap<String, String>();
        for (int i = 0; i <= SqlVariables.MAX_VARIABLES; i++) {
            tooMany.put("v" + i, "x");
        }
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SqlVariables.toSetStatements(tooMany, NOUN));
        assertTrue(ex.getMessage().contains("Too many ingestion variables"), ex.getMessage());
    }

    @Test
    void identifierAcceptsQualifiedNamesAndRejectsSqlText() {
        assertEquals("v_vars", SqlVariables.identifier("variables_view", "v_vars"));
        assertEquals("lake.main.v_vars", SqlVariables.identifier("variables_view", "lake.main.v_vars"));
        for (String bad : List.of("v_vars; DROP TABLE orders", "lake.main.v_vars WHERE 1=1",
                "\"quoted\"", "a.b.c.d", "", "1v")) {
            var ex = assertThrows(IllegalArgumentException.class,
                    () -> SqlVariables.identifier("variables_view", bad));
            assertTrue(ex.getMessage().contains("variables_view"), ex.getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> SqlVariables.identifier("variables_view", null));
    }
}
