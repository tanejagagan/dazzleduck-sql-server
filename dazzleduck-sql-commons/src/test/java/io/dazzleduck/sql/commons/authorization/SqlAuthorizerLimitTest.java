package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlAuthorizerLimitTest {

    @Test
    public void testAuthorizeWithLimit() throws UnauthorizedException {
        SqlAuthorizer authorizer = new SqlAuthorizer() {
            @Override
            public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) {
                return query;
            }

            @Override
            public boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims) {
                return false;
            }
        };

        String sql = "SELECT * FROM t";
        JsonNode query;
        try {
            query = Transformations.parseToTree(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long limitValue = 100;
        JsonNode authorized = authorizer.authorize("user", "db", "schema", query, Map.of(), limitValue, -1);

        // Verify limit is applied correctly
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS).get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        
        JsonNode limitModifier = null;
        for (JsonNode modifier : modifiers) {
            if (modifier.get(ExpressionConstants.FIELD_TYPE).asText().equals(ExpressionConstants.LIMIT_MODIFIER_TYPE)) {
                limitModifier = modifier;
                break;
            }
        }

        assertEquals(limitValue, limitModifier.get(ExpressionConstants.FIELD_LIMIT).get(ExpressionConstants.FIELD_VALUE).get(ExpressionConstants.FIELD_VALUE).asLong());
        // Offset should be missing or -1 if we follow the implementation (Transformations.addLimit doesn't add it if < 0)
        assertEquals(null, limitModifier.get(ExpressionConstants.FIELD_OFFSET));
    }
}
