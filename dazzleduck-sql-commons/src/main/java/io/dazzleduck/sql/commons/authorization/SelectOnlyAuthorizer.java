package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Authorizer that only allows read-only SQL statements (SELECT and UNION).
 * Blocks all other SQL operations including INSERT, UPDATE, DELETE, CREATE, DROP, etc.
 *
 * <p>This authorizer is useful for:
 * <ul>
 *   <li>Read-only database scenarios</li>
 *   <li>Analytics and reporting use cases</li>
 *   <li>Data export and analysis without modification</li>
 * </ul>
 *
 * <h2>Authorized Operations</h2>
 * <ul>
 *   <li><b>SELECT</b> - All SELECT queries are allowed</li>
 *   <li><b>SELECT with subqueries</b> - Nested SELECT statements are allowed</li>
 *   <li><b>UNION / UNION ALL</b> - Set operations combining SELECT results are allowed</li>
 *   <li><b>CTEs (Common Table Expressions)</b> - WITH clauses with SELECT are allowed</li>
 *   <li><b>SELECT from table functions</b> - read_parquet, read_delta, etc. are allowed</li>
 * </ul>
 *
 * <h2>Blocked Operations</h2>
 * <p>Note: DuckDB's JSON serialization ({@code json_serialize_sql()}) cannot serialize DML/DDL
 * operations (INSERT, UPDATE, DELETE, CREATE, DROP, etc.). These operations return error nodes
 * with error_type "not implemented" and message "Only SELECT statements can be serialized to json!".
 * This authorizer blocks those error nodes.</p>
 * <ul>
 *   <li><b>INSERT</b> - Data insertion is blocked</li>
 *   <li><b>UPDATE</b> - Data modification is blocked</li>
 *   <li><b>DELETE</b> - Data deletion is blocked</li>
 *   <li><b>CREATE</b> - Table creation is blocked</li>
 *   <li><b>DROP</b> - Table dropping is blocked</li>
 *   <li><b>ALTER</b> - Schema modification is blocked</li>
 *   <li><b>TRUNCATE</b> - Table truncation is blocked</li>
 *   <li><b>GRANT/REVOKE</b> - Permission changes are blocked</li>
 *   <li><b>COPY FROM</b> - Data import is blocked</li>
 *   <li><b>Any other DDL/DML</b> - Other modifications are blocked</li>
 * </ul>
 *
 * <h2>External Access Control</h2>
 * <p>When using READ_ONLY access mode, external access to tables and functions
 * ({@code read_parquet}, {@code httpfs}, {@code s3fs}, etc.) can be controlled
 * by the startup script using {@code SET enable_external_access = false}.</p>
 *
 * @see SqlAuthorizer
 */
public class SelectOnlyAuthorizer implements SqlAuthorizer {

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
        if (query.has("error") && query.get("error").asBoolean()) {
            throw new UnauthorizedException("Invalid query: " + 
                (query.has("error_message") ? query.get("error_message").asText() : "unknown error"));
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
                        "Only SELECT and UNION queries are allowed. Operation type: " + statementType
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
