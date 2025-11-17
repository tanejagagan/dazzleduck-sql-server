package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;

public class NOOPPostIngestionTaskFactoryProvider implements PostIngestionTaskFactoryProvider {

    @Override
    public void setConfig(Config config) {

    }

    @Override
    public PostIngestionTaskFactory getPostIngestionTaskFactory() {
        return connectionResult -> (PostIngestionTask) () -> {
            // Do  nothing
        };
    }
}
