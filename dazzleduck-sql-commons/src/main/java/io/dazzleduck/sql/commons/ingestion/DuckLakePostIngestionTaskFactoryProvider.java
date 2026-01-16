package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
            return ingestionResult -> PostIngestionTask.NOOP;
        }

        try {
            List<PathToTableMapping> pathToTableMappings = loadMappings(config);
            if (pathToTableMappings.isEmpty()) {
                return ingestionResult -> PostIngestionTask.NOOP;
            }

            String catalogName = pathToTableMappings.get(0).catalogName();
            String metadataDatabase = "__ducklake_metadata_" + catalogName;
            logger.info("Creating DuckLakePostIngestionTaskFactory with catalog={}, metadataDb={}, {} mapping(s)", catalogName, metadataDatabase, pathToTableMappings.size());

            return new DuckLakePostIngestionTaskFactory(pathToTableMappings, metadataDatabase, catalogName);

        } catch (Exception e) {
            return ingestionResult -> PostIngestionTask.NOOP;
        }
    }

    private List<PathToTableMapping> loadMappings(Config config) {
        if (!config.hasPath("path_to_table_mapping")) return List.of();
        return config.getConfigList("path_to_table_mapping").stream().map(c -> new PathToTableMapping(c.getString("base_path"), c.getString("table_name"), c.hasPath("schema_name") ? c.getString("schema_name") : "main", c.hasPath("catalog_name") ? c.getString("catalog_name") : "main")).toList();
    }
}