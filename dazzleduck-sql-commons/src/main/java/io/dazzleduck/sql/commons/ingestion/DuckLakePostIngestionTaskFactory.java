package io.dazzleduck.sql.commons.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for creating DuckLakePostIngestionTask instances.
 * Reads configuration from pathToTableMap to determine table settings.
 */
public class DuckLakePostIngestionTaskFactory implements PostIngestionTaskFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTaskFactory.class);

    private final Map<String, Object> pathToTableMap;
    private final String metadataDatabase;
    private final String defaultCatalogName;

    public DuckLakePostIngestionTaskFactory(Map<String, Object> pathToTableMap,
                                            String metadataDatabase,
                                            String defaultCatalogName) {
        this.pathToTableMap = pathToTableMap;
        this.metadataDatabase = metadataDatabase;
        this.defaultCatalogName = defaultCatalogName;
    }

    @Override
    public PostIngestionTask create(IngestionResult ingestionResult) {
        if (pathToTableMap.isEmpty()) {
            logger.warn("pathToTableMap is empty, returning NOOP task");
            return PostIngestionTask.NOOP;
        }

        try {
            String queueName = ingestionResult.queueName();

            // Extract table configuration from pathToTableMap
            String tableName = extractString(pathToTableMap.get("table_name"), null);
            if (tableName == null || tableName.isBlank()) {
                logger.error("No table_name found in pathToTableMap for queue {}, returning NOOP task", queueName);
                return PostIngestionTask.NOOP;
            }

            // Get catalog name - use from config or fall back to default
            String catalogName = extractString(pathToTableMap.get("catalog_name"), defaultCatalogName);

            // Get schema name - default to "main" if not specified
            String schemaName = extractString(pathToTableMap.get("schema_name"), "main");

            logger.debug("Creating DuckLakePostIngestionTask for queue={}, catalog={}, schema={}, table={}", queueName, catalogName, schemaName, tableName);

            return new DuckLakePostIngestionTask(
                    ingestionResult,
                    catalogName,
                    tableName,
                    schemaName,
                    metadataDatabase
            );

        } catch (Exception e) {
            logger.error("Failed to create DuckLakePostIngestionTask for queue {}", ingestionResult.queueName(), e);
            throw new RuntimeException("Failed to create DuckLake post-ingestion task", e);
        }
    }

    /**
     * Safely extract string value from object
     */
    private String extractString(Object obj, String defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        String value = obj.toString().trim();
        return value.isEmpty() ? defaultValue : value;
    }
}