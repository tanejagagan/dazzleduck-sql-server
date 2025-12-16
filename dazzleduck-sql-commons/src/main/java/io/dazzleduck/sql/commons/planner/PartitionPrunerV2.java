package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.delta.PartitionPruning;
import io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning;
import io.dazzleduck.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface PartitionPrunerV2 {
    Map<String, PartitionPrunerV2> staticPlanners = Map.of(
            "read_hive", new HiveSplitPlanner(),
            "read_parquet", new HiveSplitPlanner(),
            "read_delta", new DeltaLakeSplitPlanner(),
            "read_ducklake", new DucklakeSplitPlanner()
    );

    static PartitionPrunerV2 getPlanner(String functionName) {
        return staticPlanners.get(functionName);
    }

    List<FileStatus> pruneFiles(JsonNode tree,
                                long maxSplitSize,
                                Map<String, String> properties) throws SQLException, IOException;


    static String getPath(JsonNode tree) {
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);
        return catalogSchemaAndTables.get(0).tableOrPath();
    }

}


class HiveSplitPlanner implements PartitionPrunerV2 {

    @Override
    public List<FileStatus> pruneFiles(JsonNode tree, long maxSplitSize, Map<String, String> properties) throws SQLException, IOException {
        var partitionDataTypes  = Transformations.getHivePartition(tree);
        var path  = PartitionPrunerV2.getPath(tree);
        return HivePartitionPruning.pruneFiles(path,
                tree, partitionDataTypes);
    }
}

class DeltaLakeSplitPlanner implements PartitionPrunerV2 {

    @Override
    public List<FileStatus> pruneFiles(JsonNode tree, long maxSplitSize, Map<String, String> properties) throws SQLException, IOException {
        var statement = Transformations.getFirstStatementNode(tree);
        var path  = PartitionPrunerV2.getPath(tree);
        var filterExpression = Transformations.getWhereClauseForTableFunction(statement);
        return PartitionPruning.pruneFiles(path, filterExpression);
    }
}

class DucklakeSplitPlanner implements PartitionPrunerV2 {

    @Override
    public List<FileStatus> pruneFiles( JsonNode tree, long maxSplitSize, Map<String, String> properties) throws SQLException, IOException {
        var catalogSchemaAndTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(tree), null, null);
        var first = catalogSchemaAndTables.get(0);
        var catalog = first.catalog();
        var metadata = "__ducklake_metadat_" + catalog;
        try {
            return DucklakePartitionPruning.pruneFiles(first.schema(), first.tableOrPath(), tree, metadata);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}