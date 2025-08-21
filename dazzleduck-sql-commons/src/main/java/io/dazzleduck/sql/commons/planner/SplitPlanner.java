package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.delta.PartitionPruning;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static io.dazzleduck.sql.commons.ExpressionFactory.createFunction;

public interface SplitPlanner {



    public static List<List<FileStatus>> getSplits(JsonNode tree,
                                                   long maxSplitSize) throws SQLException, IOException {
        var statement = Transformations.getFirstStatementNode(tree);
        var filterExpression = Transformations.getWhereClauseForTableFunction(statement);
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);

        if (catalogSchemaAndTables.size() != 1) {
            throw new SQLException("unsupported number of tables or path in the query");
        }
        var path = catalogSchemaAndTables.getFirst().tableOrPath();
        var tableFunction =  catalogSchemaAndTables.getFirst().functionName();
        List<FileStatus> fileStatuses;
        switch (tableFunction) {
            case "read_parquet" -> {
                var partitionDataTypes  = Transformations.getHivePartition(tree);
                fileStatuses = HivePartitionPruning.pruneFiles(path,
                        tree, partitionDataTypes);
            }
            case "read_delta" ->
                    fileStatuses = PartitionPruning.pruneFiles(path, filterExpression);
            default -> throw new SQLException("unsupported type : " + tableFunction);
        }

        fileStatuses.sort(Comparator.comparing(FileStatus::lastModified));
        return getSplits(maxSplitSize, fileStatuses);
    }

    private static ArrayList<List<FileStatus>> getSplits(long maxSplitSize, List<FileStatus> fileStatuses) {
        var result = new ArrayList<List<FileStatus>>();
        var current = new ArrayList<FileStatus>();
        long currentSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            current.add(fileStatus);
            currentSize += fileStatus.size();
            if(currentSize > maxSplitSize) {
                result.add(current);
                current = new ArrayList<>();
                currentSize = 0;
            }
        }
        if(!current.isEmpty()) {
            result.add(current);
        }
        return result;
    }

    static void replacePathInFromClause(JsonNode tree, String[] paths) {
        var formatToFunction = Map.of("read_delta", "read_parquet");
        var firstStatement = Transformations.getFirstStatementNode(tree);
        var tableFunction = Transformations.getTableFunction(firstStatement);
        var format = tableFunction.get("function_name").asText();
        var functionName = formatToFunction.getOrDefault(format, format);
        var from = (ObjectNode) Transformations.getTableFunctionParent(firstStatement);
        var listChildren = new ArrayNode(JsonNodeFactory.instance);
        for (String path : paths) {
            listChildren.add(ExpressionFactory.constant(path));
        }
        var listFunction = createFunction("list_value", "main", "", listChildren);
        var parquetChildren = new ArrayNode(JsonNodeFactory.instance);
        parquetChildren.add(listFunction);
        var readParquetFunction = createFunction(functionName, "", "", parquetChildren);
        from.set("function", readParquetFunction);
    }
}
