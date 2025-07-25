package io.dazzleduck.sql.flight.server;

import org.duckdb.DuckDBResultSet;

import java.sql.*;

import static java.lang.System.lineSeparator;

/**
 * Copied from DuckDBDatabaseMetaData so that name of the columns can be customized.
 */
public interface DuckDBDatabaseMetadataUtil {

    int QUERY_SB_DEFAULT_CAPACITY = 512;
    String TRAILING_COMMA = ", ";

    public static DuckDBResultSet getCatalogs(Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return (DuckDBResultSet) statement.executeQuery(
                "SELECT DISTINCT catalog_name  FROM information_schema.schemata ORDER BY \"catalog_name\"");
    }

    public static DuckDBResultSet getSchemas(Connection conn, String catalog, String schemaPattern) throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);
        sb.append("SELECT").append(lineSeparator());

        sb.append("catalog_name AS 'catalog_name'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("schema_name AS 'db_schema_name'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("FROM information_schema.schemata").append(lineSeparator());
        sb.append("WHERE TRUE").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "catalog_name", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "schema_name", schemaPattern);
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("\"catalog_name\"").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("\"db_schema_name\"").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIdx = 0;
        if (hasCatalogParam) {
            ps.setString(++paramIdx, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(++paramIdx, schemaPattern);
        }
        ps.closeOnCompletion();
        return (DuckDBResultSet) ps.executeQuery();
    }

    private static boolean appendEqualsQual(StringBuilder sb, String colName, String value) {
        // catalog - a catalog name; must match the catalog name as it is stored in
        // the database;
        // "" retrieves those without a catalog;
        // null means that the catalog name should not be used to narrow the search
        boolean hasParam = false;
        if (value != null) {
            sb.append("AND ");
            sb.append(colName);
            if (value.isEmpty()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ?");
                hasParam = true;
            }
            sb.append(lineSeparator());
        }
        return hasParam;
    }

    private static boolean appendLikeQual(StringBuilder sb, String colName, String pattern) {
        // schemaPattern - a schema name pattern; must match the schema name as it
        // is stored in the database;
        // "" retrieves those without a schema;
        // null means that the schema name should not be used to narrow the search
        boolean hasParam = false;
        if (pattern != null) {
            sb.append("AND ");
            sb.append(colName);
            if (pattern.isEmpty()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" LIKE ? ESCAPE '\\'");
                hasParam = true;
            }
            sb.append(lineSeparator());
        }
        return hasParam;
    }

    static DuckDBResultSet getTableTypes(Connection conn) throws SQLException {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        StringBuilder stringBuilder = new StringBuilder(128);
        boolean first = true;
        for (String tableType : tableTypesArray) {
            if (!first) {
                stringBuilder.append("\nUNION ALL\n");
            }
            stringBuilder.append("SELECT '");
            stringBuilder.append(tableType);
            stringBuilder.append("'");
            if (first) {
                stringBuilder.append(" AS 'table_type'");
                first = false;
            }
        }
        stringBuilder.append("\nORDER BY table_type");
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return (DuckDBResultSet) statement.executeQuery(stringBuilder.toString());
    }


    static DuckDBResultSet getTables(Connection conn, String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        StringBuilder sb = new StringBuilder(QUERY_SB_DEFAULT_CAPACITY);

        sb.append("SELECT").append(lineSeparator());
        sb.append("table_catalog AS 'catalog_name'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_schema AS 'db_schema_name'").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_type").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("NULL::BINARY AS 'table_schema'").append(lineSeparator());
        sb.append("FROM information_schema.tables").append(lineSeparator());
        sb.append("WHERE table_name LIKE ? ESCAPE '\\'").append(lineSeparator());
        boolean hasCatalogParam = appendEqualsQual(sb, "table_catalog", catalog);
        boolean hasSchemaParam = appendLikeQual(sb, "table_schema", schemaPattern);

        if (types != null && types.length > 0) {
            sb.append("AND table_type IN (").append(lineSeparator());
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('?');
            }
            sb.append(')');
        }

        // ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
        sb.append("ORDER BY").append(lineSeparator());
        sb.append("table_type").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_catalog").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_schema").append(TRAILING_COMMA).append(lineSeparator());
        sb.append("table_name").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());

        int paramIdx = 1;
        ps.setString(paramIdx++, nullPatternToWildcard(tableNamePattern));

        if (hasCatalogParam) {
            ps.setString(paramIdx++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramIdx++, schemaPattern);
        }

        if (types != null && types.length > 0) {
            for (int i = 0; i < types.length; i++) {
                ps.setString(paramIdx + i, types[i]);
            }
        }
        ps.closeOnCompletion();
        return (DuckDBResultSet) ps.executeQuery();
    }

    private static String nullPatternToWildcard(String pattern) {
        // tableNamePattern - a tableOrPath name pattern; must match the tableOrPath name as it
        // is stored in the database
        // columnNamePattern - a column name pattern; must match the tableOrPath name as it
        // is stored in the database
        if (pattern == null) {
            // non-standard behavior.
            return "%";
        }
        return pattern;
    }
}
