package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigBasedProvider;

public interface PostIngestionTaskFactoryProvider extends ConfigBasedProvider {

    String POST_INGESTION_CONFIG_PREFIX = "post_ingestion";

    PostIngestionTaskFactoryProvider NO_OP = new NOOPPostIngestionTaskFactoryProvider();
    void setConfig(Config config);

    PostIngestionTaskFactory getPostIngestionTaskFactory();

    static PostIngestionTaskFactoryProvider load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, POST_INGESTION_CONFIG_PREFIX,
                PostIngestionTaskFactoryProvider.NO_OP);
    }
}
