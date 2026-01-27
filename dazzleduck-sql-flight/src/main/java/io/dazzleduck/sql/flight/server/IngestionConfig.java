package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;

import java.time.Duration;

public record IngestionConfig(long minBucketSize, int maxBatches, long maxPendingWrite, Duration maxDelay) {

    public static IngestionConfig fromConfig(Config config){
        return new IngestionConfig(config.getLong(ConfigConstants.MIN_BUCKET_SIZE_KEY),
                config.getInt(ConfigConstants.MAX_BATCHES_KEY),
                config.getLong(ConfigConstants.MAX_PENDING_WRITE_KEY),
                Duration.ofMillis(config.getLong(ConfigConstants.MAX_DELAY_MS_KEY)));
    }
}
