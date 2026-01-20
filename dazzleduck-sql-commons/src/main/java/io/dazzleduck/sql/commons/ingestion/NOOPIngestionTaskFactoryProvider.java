package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;

import java.nio.file.Path;

/**
 * No-operation implementation of IngestionTaskFactoryProvider.
 * Returns a factory that creates NOOP tasks which do nothing.
 */
public class NOOPIngestionTaskFactoryProvider implements IngestionTaskFactoryProvider {

    private Config config;
    private String ingestionPath;

    public NOOPIngestionTaskFactoryProvider(Config config){
        setConfig(config);
    }

    public NOOPIngestionTaskFactoryProvider(String ingestionPath){
        this.ingestionPath = ingestionPath;
    }
    @Override
    public void setConfig(Config config) {
        this.config = config;
        this.ingestionPath = config.getString("ingestion_path");
    }

    @Override
    public IngestionTaskFactory getIngestionTaskFactory() {
        return new IngestionTaskFactory() {
            @Override
            public PostIngestionTask createPostIngestionTask(IngestionResult ingestionResult) {
                return PostIngestionTask.NOOP;
            }

            @Override
            public String getTargetPath(String queueId) {
                return Path.of(ingestionPath).resolve(queueId).toString();
            }
        };

    }
}
