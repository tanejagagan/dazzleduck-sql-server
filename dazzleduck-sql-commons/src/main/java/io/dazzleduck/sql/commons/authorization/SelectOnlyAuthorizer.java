package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Authorizer that only allows read-only SQL statements (SELECT, UNION, EXPLAIN, and EXPLAIN ANALYZE).
 * Blocks all other SQL operations including INSERT, UPDATE, DELETE, CREATE, DROP, etc.
 */
public class SelectOnlyAuthorizer implements SqlAuthorizer {

    public static final String UNSUPPORTED_QUERY_TYPE_MESSAGE =
            "This query type is not supported. Only SELECT, EXPLAIN, and EXPLAIN ANALYZE queries are allowed.";

    public static SqlAuthorizer INSTANCE = new SelectOnlyAuthorizer();

    private SelectOnlyAuthorizer() {
    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        if (query == null) {
            throw new UnauthorizedException("Invalid query: node is null");
        }

        // If the query tree itself indicates an error, it's not a valid authorized query
        // (e.g. DuckDB returns {"error":true,...} for non-SELECT statements that cannot be parsed)
        if (query.has("error") && query.get("error").asBoolean()) {
            throw new UnauthorizedException(UNSUPPORTED_QUERY_TYPE_MESSAGE);
        }

        JsonNode statementsNode = query.get(ExpressionConstants.FIELD_STATEMENTS);
        if (statementsNode == null || !statementsNode.isArray()) {
            throw new UnauthorizedException("Invalid query: no statements found");
        }

        // We must authorize ALL statements in the query
        for (JsonNode statement : statementsNode) {
            JsonNode statementNode = statement.get(ExpressionConstants.FIELD_NODE);
            if (statementNode == null) {
                throw new UnauthorizedException("Invalid query: statement has no node");
            }

            JsonNode typeNode = statementNode.get(ExpressionConstants.FIELD_TYPE);
            if (typeNode == null) {
                throw new UnauthorizedException("Invalid query: statement type not found");
            }

            String statementType = typeNode.asText();

            // Only allow SELECT and SET_OPERATION (UNION) operations (read-only queries)
            // DuckDB JSON serialization also represents VALUES, DESCRIBE, SHOW as SELECT_NODE
            if (!ExpressionConstants.SELECT_NODE_TYPE.equals(statementType) &&
                !ExpressionConstants.NODE_TYPE_SET_OPERATION_NODE.equals(statementType)) {
                throw new UnauthorizedException(
                        "Only SELECT, UNION, EXPLAIN, and EXPLAIN ANALYZE queries are allowed. Operation type: " + statementType
                );
            }
        }

        // All statements are authorized
        return query;
    }

    @Override
    public boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims) {
        // Select-only authorizer denies all write access
        return false;
    }
}
