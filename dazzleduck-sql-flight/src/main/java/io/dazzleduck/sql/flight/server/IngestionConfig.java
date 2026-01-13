package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.util.ConfigUtils;

import java.time.Duration;

public record IngestionConfig(long minBucketSize, Duration maxDelay) {

    public static IngestionConfig fromConfig(Config config){
        return new IngestionConfig(config.getLong(ConfigUtils.MIN_BUCKET_SIZE_KEY),
                Duration.ofMillis(config.getLong(ConfigUtils.MAX_DELAY_MS_KEY)));
    }
}
