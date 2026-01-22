package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.LinkedHashMap;

/**
 * Provider for DuckLakeIngestionTaskFactory.
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
        return new DuckLakeIngestionTaskFactory(pathToTableMappings);
    }


    private Map<String, QueueIdToTableMapping> loadMappings(Config config) {
        if (!config.hasPath("ingestion_queue_table_mapping")) return Map.of();
        Map<String, QueueIdToTableMapping> mappings = new LinkedHashMap<>();
        config.getConfigList("ingestion_queue_table_mapping").forEach(c -> {
            Map<String, String> additionalParameters = new LinkedHashMap<>();
            if (c.hasPath("additional_parameters")) {
                c.getConfig("additional_parameters").entrySet().forEach(e ->
                    additionalParameters.put(e.getKey(), e.getValue().unwrapped().toString()));
            }
            QueueIdToTableMapping mapping = new QueueIdToTableMapping(
                    c.getString("ingestion_queue"),
                    c.getString("catalog"),
                    c.getString("schema"),
                    c.getString("table"),
                    additionalParameters);
            mappings.put(mapping.ingestionQueue(), mapping);
        });
        return mappings;
    }
}