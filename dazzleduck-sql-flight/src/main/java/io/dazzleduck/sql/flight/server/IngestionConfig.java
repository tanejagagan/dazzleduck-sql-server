package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;

import java.time.Duration;

/**
 * @deprecated Use {@link io.dazzleduck.sql.commons.ingestion.IngestionConfig}.
 *             <p><b>Source-incompatible</b> since the refresh-delay removal: the constructor lost
 *             its trailing {@code Duration configRefreshDelay} parameter and the
 *             {@code configRefreshDelay()} accessor is gone, because nothing read the value. A
 *             call site passing it must drop that argument.
 *             <p><b>Binary-incompatible</b>: this was previously a {@code record}; it is now a
 *             {@code final class}. Pre-compiled artifacts that pattern-match on it as a record
 *             or use its component accessors reflectively must be recompiled.
 */
@Deprecated
public final class IngestionConfig {

    private final io.dazzleduck.sql.commons.ingestion.IngestionConfig delegate;

    public IngestionConfig(long minBucketSize, long maxBucketSize, int maxBatches,
                           long maxPendingWrite, Duration maxDelay) {
        this(new io.dazzleduck.sql.commons.ingestion.IngestionConfig(
                minBucketSize, maxBucketSize, maxBatches, maxPendingWrite, maxDelay));
    }

    private IngestionConfig(io.dazzleduck.sql.commons.ingestion.IngestionConfig delegate) {
        this.delegate = delegate;
    }

    public long     minBucketSize()    { return delegate.minBucketSize(); }
    public long     maxBucketSize()    { return delegate.maxBucketSize(); }
    public int      maxBatches()       { return delegate.maxBatches(); }
    public long     maxPendingWrite()  { return delegate.maxPendingWrite(); }
    public Duration maxDelay()         { return delegate.maxDelay(); }
    public String   parquetCompression(){ return delegate.parquetCompression(); }

    public static IngestionConfig fromConfig(Config config) {
        return new IngestionConfig(io.dazzleduck.sql.commons.ingestion.IngestionConfig.fromConfig(config));
    }

    /** Converts to the canonical commons type. */
    public io.dazzleduck.sql.commons.ingestion.IngestionConfig toCommonsConfig() {
        return delegate;
    }
}
