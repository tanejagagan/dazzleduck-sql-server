package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Claim based Authorizer which respect all the data showing up in the jwt  claim
 * Following claims are required
 * filter = 'a > 10'
 * columns = [a, b, c]
 * Path based access
 *  path = '/a/b/c'
 *  function = 'parquet, json, csv'
 * Or Identifier based access
 *  identifier = catalog.schema.table
 */
public class JwtClaimBasedAuthorizer implements SqlAuthorizer {

    public static SqlAuthorizer INSTANCE = new JwtClaimBasedAuthorizer();

    private JwtClaimBasedAuthorizer() {

    }

    private JsonNode withUpdatedDatabaseSchema(JsonNode tree, String database, String schema ) {
        var copy = tree.deepCopy();
        var f = Transformations.identity().andThen(Transformations::getFirstStatementNode).apply(copy);
        var fromTable = (ObjectNode)f.get(ExpressionConstants.FIELD_FROM_TABLE);
        var type = fromTable.get(ExpressionConstants.FIELD_TYPE).asText();
        if( type.equals( ExpressionConstants.BASE_TABLE_TYPE)) {
            fromTable.put(ExpressionConstants.FIELD_CATALOG_NAME, database);
            fromTable.put(ExpressionConstants.FIELD_SCHEMA_NAME,  schema);
            return copy;
        }
        return tree;
    }
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) throws UnauthorizedException {
        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.size() != 1) {
            throw new UnauthorizedException("%s TableOrPath/Path found: Only one table or path is supported".formatted(catalogSchemaTables.size()));
        }

        var updatedQuery = withUpdatedDatabaseSchema(query, database, schema );
        var catalogSchemaTable = catalogSchemaTables.get(0);
        var path = verifiedClaims.get(Headers.HEADER_PATH);
        var functionName = verifiedClaims.get(Headers.HEADER_FUNCTION);
        var table = verifiedClaims.get(Headers.HEADER_TABLE);
        if (path == null && table == null) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }

        // Check function access
        if (catalogSchemaTable.type() == Transformations.TableType.TABLE_FUNCTION &&
                (path == null || !SqlAuthorizer.hasAccessToPath(path, catalogSchemaTable.tableOrPath())) &&
                !SqlAuthorizer.hasAccessToTableFunction(functionName, catalogSchemaTable.functionName())){
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }


        // Check table access
        if (catalogSchemaTable.type() == Transformations.TableType.BASE_TABLE &&
                (table == null || !SqlAuthorizer.hasAccessToTable(database, schema, table, catalogSchemaTable))){
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }
        var filter = verifiedClaims.get(Headers.HEADER_FILTER);
        if (filter == null) {
            return updatedQuery;
        }
        JsonNode compiledFilter = SqlAuthorizer.compileFilterString(filter);
        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return SqlAuthorizer.addFilterToTableFunction(updatedQuery, compiledFilter);
            }
            case BASE_TABLE -> {
                return SqlAuthorizer.addFilterToBaseTable(updatedQuery, compiledFilter);
            }
            default -> {
                return null;
            }
        }
    }

    @Override
    public boolean hasWriteAccess(String user, String path, Map<String, String> verifiedClaims) {
        var accessType = verifiedClaims.get(Headers.HEADER_ACCESS_TYPE);
        if (!AccessType.WRITE.name().equalsIgnoreCase(accessType)) {
            return false;
        }
        var authorizedPath = verifiedClaims.get(Headers.HEADER_PATH);
        if (authorizedPath == null) {
            return false;
        }
        return SqlAuthorizer.hasAccessToPath(authorizedPath, path);
    }
}
