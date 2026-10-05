package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigConstants;

import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * Queue tuning parameters for a {@link ParquetIngestionQueue}.
 *
 * <p>Separates operational concerns (flush thresholds, delays, output codec) from domain concerns
 * (output path, transformation, partition columns) which are provided by
 * {@link IngestionHandler}.
 *
 * <p>How often that handler re-reads its domain state is the handler's own setting, not one of
 * these: {@code queue_config_refresh_delay_ms} is read from the
 * {@code ingestion_task_factory_provider} block by
 * {@link DuckLakeIngestionTaskFactoryProvider} (the dynamic provider uses
 * {@code config_load_interval_ms}). This record used to carry a copy of it that nothing read.
 *
 * @param parquetCompression codec for Parquet files the queue writes; supplied by config
 *                           (defaults to snappy), or {@code null} to leave it to DuckDB.
 *                           Independent of the DuckLake catalog's own
 *                           {@code parquet_compression}, which governs compaction rewrites.
 */
public record IngestionConfig(long minBucketSize,
                               long maxBucketSize,
                               int  maxBatches,
                               long maxPendingWrite,
                               Duration maxDelay,
                               String parquetCompression) {

    public static final long     DEFAULT_MAX_BUCKET_SIZE   = 100L * 1024 * 1024; // 100 MB
    public static final long     DEFAULT_MAX_PENDING_WRITE = 500L * 1024 * 1024; // 500 MB
    public static final int      DEFAULT_MAX_BATCHES       = Integer.MAX_VALUE;

    private static final List<String> PARQUET_CODECS =
            List.of("brotli", "gzip", "lz4", "lz4_raw", "snappy", "uncompressed", "zstd");

    /** Fails at config load rather than at the first write. */
    public IngestionConfig {
        if (parquetCompression != null) {
            String codec = parquetCompression.toLowerCase(Locale.ROOT);
            if (!PARQUET_CODECS.contains(codec)) {
                throw new IllegalArgumentException(
                        "Unsupported %s '%s'; supported values are %s".formatted(
                                ConfigConstants.PARQUET_COMPRESSION_KEY, parquetCompression, PARQUET_CODECS));
            }
            parquetCompression = codec;
        }
    }

    public IngestionConfig(long minBucketSize, long maxBucketSize, int maxBatches,
                           long maxPendingWrite, Duration maxDelay) {
        this(minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay, null);
    }

    public static IngestionConfig fromConfig(Config config) {
        return new IngestionConfig(
                config.getLong(ConfigConstants.MIN_BUCKET_SIZE_KEY),
                config.hasPath(ConfigConstants.MAX_BUCKET_SIZE_KEY)
                        ? config.getLong(ConfigConstants.MAX_BUCKET_SIZE_KEY) : DEFAULT_MAX_BUCKET_SIZE,
                config.hasPath(ConfigConstants.MAX_BATCHES_KEY)
                        ? config.getInt(ConfigConstants.MAX_BATCHES_KEY)      : DEFAULT_MAX_BATCHES,
                config.hasPath(ConfigConstants.MAX_PENDING_WRITE_KEY)
                        ? config.getLong(ConfigConstants.MAX_PENDING_WRITE_KEY) : DEFAULT_MAX_PENDING_WRITE,
                Duration.ofMillis(config.getLong(ConfigConstants.MAX_DELAY_MS_KEY)),
                config.getString(ConfigConstants.PARQUET_COMPRESSION_KEY));
    }
}
