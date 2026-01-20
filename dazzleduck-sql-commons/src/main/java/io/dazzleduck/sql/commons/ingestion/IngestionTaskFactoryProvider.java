package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.config.ConfigBasedProvider;

public interface IngestionTaskFactoryProvider extends ConfigBasedProvider {


    void setConfig(Config config);

    IngestionTaskFactory getIngestionTaskFactory();

}
