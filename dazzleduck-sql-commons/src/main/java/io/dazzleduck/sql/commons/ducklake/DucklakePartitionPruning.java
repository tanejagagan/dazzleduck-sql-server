package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DucklakePartitionPruning {

    private static final String NESTED = "{'key' : concat('min_', column_id), 'value' : min_value }," +
                    " {'key' : concat('max_', column_id), 'value' :  max_value}," +
                    " {'key' : concat('null_count_', column_id), 'value' : cast(null_count as varchar)}," +
                    " {'key' : concat('contains_nan_', column_id), 'value' : cast(contains_nan as varchar)}";
    private static final List<String> NESTED_COLUMN_KEYS = List.of("min", "max", "null_count", "contains_nan");


    private static final String COLUMN_INFO_QUERY =
            " WITH s AS (SELECT schema_id FROM %s.ducklake_schema  WHERE end_snapshot IS NULL AND schema_name = '%s'),\n" +
            " t AS (SELECT table_id, schema_id FROM %s.ducklake_table WHERE end_snapshot IS NULL AND table_name = '%s'),\n" +
            " c AS  (SELECT  column_id, table_id, column_name, column_type FROM %s.ducklake_column WHERE end_snapshot IS NULL ),\n" +
            " sv AS (SELECT max(schema_version) as version FROM %s.ducklake_snapshot )\n" +
            " SELECT c.column_id as id, c.column_name as name, c.column_type as \"type\", sv.version as schemaVersion FROM c, s, t, sv WHERE c.table_id = t.table_id AND  s.schema_id = t.schema_id ;\n";


    private static final String SCHEMA_VERSION_QUERY = "SELECT max(schema_version) FROM %s.ducklake_snapshot";

    private static final String TABLE_ID_QUERY ="select table_id from %s.ducklake_table t, %s.ducklake_schema s " +
            "where s.schema_name = '%s' and t.table_name = '%s'" +
            "and s.schema_id  = t.schema_id";

    private static final String QUERY =
            " WITH L AS (SELECT data_file_id, path, path_is_relative, file_format,record_count, file_size_bytes, mapping_id FROM %s.ducklake_data_file WHERE table_id = %s and end_snapshot is null),\n" +
                    "M AS (SELECT mapping_id, type FROM %s.ducklake_column_mapping WHERE table_id = %s),\n"  +
                    "B AS (SELECT data_file_id, unnest([" + NESTED + "]) AS  nested, column_id FROM %s.ducklake_file_column_stats WHERE table_id = %s and column_id in (%s)),\n" +
                    " AA AS (SELECT  data_file_id, nested.key as column_id, nested.value as value, column_id from B),\n" +
                    " P AS (PIVOT AA ON column_id IN (%s) USING first(value) GROUP BY data_file_id),\n" +
                    " R AS (%s)\n" +
                    " SELECT L.path, L.file_size_bytes, cast(0  as  bigint) FROM L INNER JOIN R ON L.data_file_id = R.data_file_id LEFT OUTER JOIN M ON M.mapping_id = L.mapping_id";
                    //" SELECT L.path, L.path_is_relative, L.file_format, L.record_count, L.file_size_bytes, M.type FROM L INNER JOIN R ON L.data_file_id = R.data_file_id LEFT OUTER JOIN M ON M.mapping_id = L.mapping_id";

    public static String partitionSql(String innerSql, Long tableId, List<Long> columnId, String metadataDatabase) {
        var columnString = columnId.stream().map( l -> "min_%s, max_%s".formatted(l, l)).collect(Collectors.joining(","));
        var columnIdString = columnId.stream().map("%s"::formatted).collect(Collectors.joining(","));
        return QUERY.formatted(metadataDatabase, tableId, metadataDatabase, tableId, metadataDatabase, tableId, columnIdString, columnString, innerSql);
    }
    public static String  getColumnInfoQuery(String schema, String table, String metadataDatabase) {
        return COLUMN_INFO_QUERY.formatted(metadataDatabase, schema, metadataDatabase, table, metadataDatabase, metadataDatabase);
    }

    public static String  getSchemaVersionQuery(String metadataDatabase) {
        return SCHEMA_VERSION_QUERY.formatted(metadataDatabase);
    }

    public static String getTableIdQuery(String schema, String table, String metadataDatabase) {
        return TABLE_ID_QUERY.formatted(metadataDatabase, metadataDatabase, schema, table);
    }
    public static List<FileStatus> pruneFiles(String basePath,
                                              String filter,
                                              String[][] partitionDataTypes,
                                              Map<String, Long> columnIds) {
        // get all the columns in the filter
        // Get all value
        long[] relevantColumnId = new long[0];
        return null;
    }
    public record ColumnInfo(Long id, String name, String type, Long schemaVersion){ }
    public record VersionEntity<E>(long version, E entity){}
    public static Map<String, VersionEntity<Map<String, ColumnInfo>>> tableIds = new ConcurrentHashMap<>();

    private static Map<String, ColumnInfo> getColumnIdMap(String schema, String table, String metadataDatabase) throws SQLException {
        var schemaTable = "%s.%s".formatted(schema, table);
        var schemaVersionQuery = getSchemaVersionQuery(metadataDatabase);
        var latestSchemaVersion = ConnectionPool.collectFirst(schemaVersionQuery, Long.class);
        var versionEntity = tableIds.compute(schemaTable, (key, oldValue) -> {
            if(oldValue == null || oldValue.version() < latestSchemaVersion ){
                List<ColumnInfo> info = null;
                try {
                    info = getInfo(schema, table, metadataDatabase);
                } catch ( NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
                var version = 0L;
               var res = new HashMap<String, ColumnInfo>();
               for(var i :  info){
                   version = Math.max(version, i.schemaVersion);
                   res.put(i.name, i);
               }
               return new VersionEntity<>(version, res);
            }  else {
                return oldValue;
            }
        });
        return versionEntity.entity;
    }

    private static List<ColumnInfo> getInfo(String schema, String table, String metadataDatabase) throws  NoSuchMethodException {
        var query =  getColumnInfoQuery(schema, table, metadataDatabase);
        try(var connection = ConnectionPool.getConnection()){
            return (List<ColumnInfo>)ConnectionPool.collectAll(connection, query, ColumnInfo.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    private static Function<JsonNode, JsonNode> start(Function<JsonNode, JsonNode> start){
        return start;
    }

    public static List<FileStatus> pruneFiles(String schema,
                                              String table,
                                              JsonNode tree,
                                              String metadataDatabase) throws SQLException, NoSuchMethodException, JsonProcessingException {

        var columnMap = getColumnIdMap(schema, table, metadataDatabase);
        var tableId = getTableId(schema, table, metadataDatabase);
        var maxMap = new HashMap<String, String>();
        var minMap = new HashMap<String, String>();
        var typeMap = new HashMap<String, String>();
        for(var e : columnMap.entrySet()) {
            minMap.put(e.getKey(), "min_" + e.getValue().id());
            maxMap.put(e.getKey(), "max_" + e.getValue().id());
            typeMap.put(e.getKey(), e.getValue().type());
        }

        var statTable = "P";
        var partitionQuery = Transformations.replaceEqualMinMaxInQuery(statTable, minMap, maxMap, typeMap)
                .apply(tree);
        var where = Transformations.identity()
                .andThen(Transformations::getFirstStatementNode)
                .andThen(Transformations::getWhereClauseForBaseTable)
                .apply(tree);
        var references = Transformations.collectReferences(where);
        var columnIds = references.stream().map(n -> columnMap.get(Transformations.getReferenceName(n)[0]).id())
                .toList();

        var partitionSql = Transformations.parseToSql(partitionQuery);
        var toRun = partitionSql(partitionSql, tableId, columnIds,metadataDatabase);
        try (var connection = ConnectionPool.getConnection()) {
            var res = ConnectionPool.collectAll(connection, toRun, FileStatus.class );
            return (List<FileStatus>) res;
        }

    }
    public static List<FileStatus> pruneFiles(String schema,
                                              String table,
                                              String sql,
                                              String metadataDatabase) throws SQLException, NoSuchMethodException, JsonProcessingException {
        var tree = Transformations.parseToTree(sql);
        return pruneFiles(schema, table, tree, metadataDatabase);
    }

    private static Long getTableId(String schema, String table, String metadataDatabase) throws SQLException {
        var query = getTableIdQuery(schema, table, metadataDatabase).formatted(schema, table);
        return ConnectionPool.collectFirst(query, Long.class);
    }

    public static void main(String[] args) {
        //System.out.printf(QUERY, "min_1, min_2, min_3, max_1, max_2, max_3", "true");
    }

    public static String constructQuery(String metadataDatabase,
                                        long tableId,
                                        List<Long> relevantColumnIds) {
        var relevantColumnMinMaxString = relevantColumnIds
                .stream()
                .map(l -> NESTED_COLUMN_KEYS.stream()
                        .map(k -> k + "_" + l)
                        .collect(Collectors.joining(",")))
                .collect(Collectors.joining(","));
        String relevantColumnString = relevantColumnIds.stream().map(Object::toString).collect(Collectors.joining(","));
        return QUERY.formatted(metadataDatabase, tableId, metadataDatabase, tableId, metadataDatabase, tableId, relevantColumnString, relevantColumnMinMaxString, "true");
    }
}
