package io.dazzleduck.sql.otel.collector.query;

import com.typesafe.config.Config;

import java.time.Duration;

/**
 * The {@code otel_collector.query} block: a local SQL endpoint on the collector's own DuckDB
 * instance, for testing (see {@link QueryServer}).
 *
 * @param enabled        global switch; the server is not started when false
 * @param host           address to bind; localhost by default, since the endpoint has no auth
 * @param port           port to listen on; 0 picks a free one
 * @param threads        request threads, which also bounds concurrent queries
 * @param timeout        a query still running after this is cancelled
 * @param arrowBatchSize rows per Arrow batch read from DuckDB
 */
public record QuerySettings(boolean enabled, String host, int port, int threads, Duration timeout, int arrowBatchSize) {

    public static QuerySettings disabled() {
        return new QuerySettings(false, "127.0.0.1", 8082, 4, Duration.ofSeconds(30), 10_000);
    }

    /** Parses the block strictly: a wrong type or an invalid value fails startup. */
    public static QuerySettings from(Config c) {
        var defaults = disabled();
        var settings = new QuerySettings(
                c.hasPath("enabled") && c.getBoolean("enabled"),
                c.hasPath("host") ? c.getString("host") : defaults.host(),
                c.hasPath("port") ? c.getInt("port") : defaults.port(),
                c.hasPath("threads") ? c.getInt("threads") : defaults.threads(),
                c.hasPath("timeout") ? c.getDuration("timeout") : defaults.timeout(),
                c.hasPath("arrow_batch_size") ? c.getInt("arrow_batch_size") : defaults.arrowBatchSize());
        if (settings.host().isBlank()) {
            throw new IllegalArgumentException("query.host must not be blank");
        }
        if (settings.port() < 0 || settings.port() > 65535) {
            throw new IllegalArgumentException("query.port must be between 0 and 65535, got " + settings.port());
        }
        if (settings.threads() <= 0) {
            throw new IllegalArgumentException("query.threads must be positive, got " + settings.threads());
        }
        if (settings.timeout().isZero() || settings.timeout().isNegative()) {
            throw new IllegalArgumentException("query.timeout must be positive, got " + settings.timeout());
        }
        if (settings.arrowBatchSize() <= 0) {
            throw new IllegalArgumentException("query.arrow_batch_size must be positive, got " + settings.arrowBatchSize());
        }
        return settings;
    }
}
