package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Following claims are required
 * filter = 'a > 10'
 * columns = [a, b, c]
 * Path based access
 * path = '/a/b/c'
 * function = 'parquet, json, csv'
 * Or Identifier based access
 * identifier = catalog.schema.table
 */
public class ClaimBasedAuthorizer implements SqlAuthorizer {
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) throws UnauthorizedException {

        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.size() != 1) {
            throw new UnauthorizedException("%s TableOrPath/Path found: Only one table or path is supported".formatted(catalogSchemaTables.size()));
        }
        var catalogSchemaTable = catalogSchemaTables.get(0);
        var path = verifiedClaims.get("path");
        var functionName = verifiedClaims.get("function");
        var identifier = verifiedClaims.get("identifier");
        if (path != null &&
                (catalogSchemaTable.type() != Transformations.TableType.TABLE_FUNCTION ||
                        !catalogSchemaTable.functionName().equals(functionName))) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }
        if (identifier != null && identifier.equals("%s.%s.%s".formatted(catalogSchemaTable.catalog(), catalogSchemaTable.schema(), catalogSchemaTable.tableOrPath()))) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }
        var filter = verifiedClaims.get("filter");
        JsonNode compiledFiler = SqlAuthorizer.compileFilterString(filter);
        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return SqlAuthorizer.addFilterToTableFunction(query, compiledFiler);
            }
            case BASE_TABLE -> {
                return SqlAuthorizer.addFilterToBaseTable(query, compiledFiler);
            }
            default -> {
                return null;
            }
        }
    }
}
