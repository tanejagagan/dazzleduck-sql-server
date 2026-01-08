package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Provider for DuckLakePostIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakePostIngestionTaskFactoryProvider implements PostIngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakePostIngestionTaskFactoryProvider.class);

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public PostIngestionTaskFactory getPostIngestionTaskFactory() {
        if (config == null) {
            logger.warn("Config is null, returning NOOP factory");
            return ingestionResult -> PostIngestionTask.NOOP;
        }

        try {
            // Get catalog name from config
            String catalogName = config.hasPath("catalog_name") ? config.getString("catalog_name") : config.hasPath("database") ? config.getString("database") : "main";
            // Get metadata database (optional)
            String metadataDatabase = config.hasPath("metadata_database") ? config.getString("metadata_database") : null;
            Map<String, Object> pathToTableMap = buildPathToTableMap();
            logger.info("Creating DuckLakePostIngestionTaskFactory with catalog={}, metadataDb={}, mappings={}", catalogName, metadataDatabase, pathToTableMap);

            return new DuckLakePostIngestionTaskFactory(pathToTableMap, metadataDatabase, catalogName);

        } catch (Exception e) {
            logger.error("Failed to create DuckLakePostIngestionTaskFactory, returning NOOP", e);
            return ingestionResult -> PostIngestionTask.NOOP;
        }
    }

    /**
     * Builds the path-to-table mapping from configuration.
     * Expected config structure:
     * <pre>
     * path_to_table_mapping {
     *   table_name = "logs"
     *   catalog_name = "my_ducklake"  // Optional, defaults to root config
     *   schema_name = "main"          // Optional, defaults to "main"
     *   base_path = "/data/logs"      // Optional
     * }
     * </pre>
     */
    private Map<String, Object> buildPathToTableMap() {
        Map<String, Object> pathToTableMap = new HashMap<>();

        if (!config.hasPath("path_to_table_mapping")) {
            logger.warn("No path_to_table_mapping found in config");
            return pathToTableMap;
        }

        try {
            Config mappingConfig = config.getConfig("path_to_table_mapping");

            // Extract all configuration values
            for (String key : mappingConfig.root().keySet()) {
                try {
                    Object value = extractConfigValue(mappingConfig, key);
                    if (value != null) {
                        pathToTableMap.put(key, value);
                        logger.debug("Added mapping: {} = {}", key, value);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to extract config value for key: {}", key, e);
                }
            }

        } catch (Exception e) {
            logger.error("Error building path to table map", e);
        }

        return pathToTableMap;
    }

    /**
     * Extract configuration value with appropriate type
     */
    private Object extractConfigValue(Config config, String key) {
        try {
            // Try different types based on the value type
            switch (config.getValue(key).valueType()) {
                case NUMBER:
                    return config.getLong(key);
                case BOOLEAN:
                    return config.getBoolean(key);
                case LIST:
                    return config.getStringList(key);
                case STRING:
                default:
                    return config.getString(key);
            }
        } catch (Exception e) {
            logger.warn("Failed to extract typed value for key: {}, returning as string", key);
            try {
                return config.getString(key);
            } catch (Exception ex) {
                logger.error("Failed to extract value for key: {}", key, ex);
                return null;
            }
        }
    }
}