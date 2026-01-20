package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Provider for DuckLakePostIngestionTaskFactory.
 * Reads configuration and creates factory instances for DuckLake table ingestion.
 */
public class DuckLakeIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private static final Logger logger = LoggerFactory.getLogger(DuckLakeIngestionTaskFactoryProvider.class);

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public IngestionTaskFactory getIngestionTaskFactory() {
        Map<String, QueueIdToTableMapping> pathToTableMappings = loadMappings(config);
        return new DuckLakePostIngestionTaskFactory(pathToTableMappings);
    }


    private Map<String, QueueIdToTableMapping> loadMappings(Config config) {
        if (!config.hasPath("ingestion_queue_to_table_mapping")) return Map.of();
        Map<String, QueueIdToTableMapping> mappings = new LinkedHashMap<>();
        config.getConfigList("ingestion_queue_to_table_mapping").forEach(c -> {
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(c.getString("ingestion_queue_id"), c.getString("table_name"), c.getString("schema_name"), c.getString("catalog_name"));
            mappings.put(mapping.queueId(), mapping);
        });
        return mappings;
    }
}