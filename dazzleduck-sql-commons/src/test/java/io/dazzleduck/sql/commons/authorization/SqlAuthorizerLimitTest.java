package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlAuthorizerLimitTest {

    private static final SqlAuthorizer PASSTHROUGH = new SqlAuthorizer() {
        @Override
        public JsonNode authorize(String user, String database, String schema,
                                  JsonNode query, Map<String, String> verifiedClaims) {
            return query;
        }
        @Override
        public boolean hasWriteAccess(String user, String ingestionQueue,
                                      Map<String, String> verifiedClaims) {
            return false;
        }
    };

    private static long extractLimit(JsonNode authorized) {
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        for (JsonNode modifier : modifiers) {
            if (modifier.get(ExpressionConstants.FIELD_TYPE).asText()
                    .equals(ExpressionConstants.LIMIT_MODIFIER_TYPE)) {
                return modifier.get(ExpressionConstants.FIELD_LIMIT)
                        .get(ExpressionConstants.FIELD_VALUE)
                        .get(ExpressionConstants.FIELD_VALUE).asLong();
            }
        }
        throw new AssertionError("No LIMIT_MODIFIER found in: " + authorized);
    }

    @Test
    public void testAddLimit_baseTable() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 100L, -1);
        assertEquals(100L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_tableFunction() throws Exception {
        // generate_series is a TABLE_FUNCTION — previously addLimit returned the query unchanged
        JsonNode query = Transformations.parseToTree("SELECT * FROM generate_series(1, 1000)");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 3L, -1);
        assertEquals(3L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_subquery() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM (SELECT * FROM t) sub");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 5L, -1);
        assertEquals(5L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_noLimit_noOffset_unchanged() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), -1L, -1L);
        // No LIMIT modifier should be added
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        boolean hasLimit = modifiers != null && modifiers.size() > 0;
        assertEquals(false, hasLimit, "Expected no LIMIT modifier when limit=-1 and offset=-1");
    }
}
