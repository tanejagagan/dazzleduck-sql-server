package io.dazzleduck.sql.commons.ducklake;

import io.dazzleduck.sql.commons.FileStatus;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DucklakePartitionPruning {

    private static final String NESTED = "{'key' : concat('min_', column_id), 'value' : min_value }," +
                    " {'key' : concat('max_', column_id), 'value' :  max_value}," +
                    " {'key' : concat('null_count_', column_id), 'value' : cast(null_count as varchar)}," +
                    " {'key' : concat('contains_nan_', column_id), 'value' : cast(contains_nan as varchar)}";
    private static final List<String> NESTED_COLUMN_KEYS = List.of("min", "max", "null_count", "contains_nan");
    private static final String QUERY =
            " WITH L AS (SELECT data_file_id, path, path_is_relative, file_format,record_count, file_size_bytes, mapping_id FROM %s.ducklake_data_file WHERE table_id = %s and end_snapshot is null)," +
                    "M AS (SELECT mapping_id, type FROM %s.ducklake_column_mapping WHERE table_id = %s), "  +
                    "B AS (SELECT data_file_id, unnest([" + NESTED + "]) AS  nested, column_id FROM %s.ducklake_file_column_stats WHERE table_id = %s and column_id in (%s))," +
                    " AA AS (SELECT  data_file_id, nested.key as column_id, nested.value as value, column_id from B)," +
                    " P AS (PIVOT AA ON column_id IN (%s) USING first(value) GROUP BY data_file_id)," +
                    " R AS (SELECT * FROM P WHERE %s)" +
                    " SELECT L.path, L.path_is_relative, L.file_format, L.record_count, L.file_size_bytes, M.type FROM L INNER JOIN R ON L.data_file_id = R.data_file_id LEFT OUTER JOIN M ON M.mapping_id = L.mapping_id";

    public static List<FileStatus> pruneFiles(String basePath,
                                              String filter,
                                              String[][] partitionDataTypes,
                                              Map<String, Long> columnIds) {
        // get all the columns in the filter
        // Get all value
        long[] relevantColumnId = new long[0];
        return null;
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
