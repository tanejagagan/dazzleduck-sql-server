package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for creating DuckLakePostIngestionTask instances.
 * Uses a map for fast lookup based on ingestion path.
 */
public class DuckLakePostIngestionTaskFactory implements PostIngestionTaskFactory {
    private final Map<String, PathToTableMapping> pathToTableMappings;
    private final String metadataDatabase;
    private final String defaultCatalogName;

    public DuckLakePostIngestionTaskFactory(Map<String, PathToTableMapping> pathToTableMappings,
                                            String metadataDatabase,
                                            String defaultCatalogName
    ) {
        this.pathToTableMappings = pathToTableMappings;
        this.metadataDatabase = metadataDatabase;
        this.defaultCatalogName = defaultCatalogName;
    }

    @Override
    public PostIngestionTask create(IngestionResult result) {
        PathToTableMapping mapping = pathToTableMappings.get(result.queueName());
        // Fallback: try suffix (last path segment)
        if (mapping == null) {
            mapping = pathToTableMappings.get(extractSuffix(result.queueName()));
        }

        if (mapping == null) {
            return PostIngestionTask.NOOP;
        }

        return new DuckLakePostIngestionTask(result, defaultCatalogName, mapping.tableName(), mapping.schemaName(), metadataDatabase);
    }

    // extracting last path segment of queueName (eg. log, metric)
    private String extractSuffix(String queueName) {
        String normalized = queueName.replace("\\", "/");
        int lastSlash = normalized.lastIndexOf('/');
        return lastSlash >= 0 ? normalized.substring(lastSlash + 1) : normalized;
    }
}