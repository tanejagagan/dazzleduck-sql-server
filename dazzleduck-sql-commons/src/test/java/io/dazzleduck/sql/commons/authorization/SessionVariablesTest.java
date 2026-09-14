package io.dazzleduck.sql.commons.authorization;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionVariablesTest {

    @Test
    void rendersOneSetStatementPerEntryInOrder() {
        List<String> sqls = SessionVariables.toSetStatements(
                "{\"tenant_id\":\"acme\",\"region\":\"us-east\"}");
        assertEquals(List.of(
                "SET VARIABLE tenant_id = 'acme'",
                "SET VARIABLE region = 'us-east'"), sqls);
    }

    @Test
    void nullBlankAndEmptyObjectYieldNoStatements() {
        assertTrue(SessionVariables.toSetStatements(null).isEmpty());
        assertTrue(SessionVariables.toSetStatements("   ").isEmpty());
        assertTrue(SessionVariables.toSetStatements("{}").isEmpty());
    }

    @Test
    void numberValueIsRejectedWithQuoteHint() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"var1\":42}"));
        assertTrue(ex.getMessage().contains("needs to be inside quotes"), ex.getMessage());
        assertTrue(ex.getMessage().contains("42"), ex.getMessage());
    }

    @Test
    void booleanValueIsRejectedWithQuoteHint() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"active\":true}"));
        assertTrue(ex.getMessage().contains("needs to be inside quotes"), ex.getMessage());
    }

    @Test
    void quotedNumberIsAcceptedAsAString() {
        assertEquals(List.of("SET VARIABLE var1 = '42'"),
                SessionVariables.toSetStatements("{\"var1\":\"42\"}"));
    }

    @Test
    void nullValuedEntriesAreSkipped() {
        assertTrue(SessionVariables.toSetStatements("{\"unset\":null}").isEmpty());
    }

    @Test
    void singleQuotesInValueAreDoubled() {
        List<String> sqls = SessionVariables.toSetStatements("{\"name\":\"O'Brien\"}");
        assertEquals(List.of("SET VARIABLE name = 'O''Brien'"), sqls);
    }

    @Test
    void sqlInjectionInValueIsNeutralizedIntoASingleLiteral() {
        // A crafted value must stay entirely inside one quoted literal, never break out into SQL.
        List<String> sqls = SessionVariables.toSetStatements(
                "{\"tenant_id\":\"x'; DROP TABLE orders; --\"}");
        assertEquals(List.of("SET VARIABLE tenant_id = 'x''; DROP TABLE orders; --'"), sqls);
    }

    @Test
    void invalidVariableNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"tenant id\":\"acme\"}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"1tenant\":\"acme\"}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"drop;table\":\"x\"}"));
    }

    @Test
    void nestedJsonValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"a\":{\"b\":1}}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"a\":[1,2]}"));
    }

    @Test
    void malformedJsonIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("not json"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("[\"a\",\"b\"]"));
    }
}
