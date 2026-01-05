package io.dazzleduck.sql.micrometer.metrics;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;

/**
 * Factory for creating MeterRegistry instances configured from application.conf.
 *
 * <p>Expected configuration format in application.conf:</p>
 * <pre>{@code
 * dazzleduck_micrometer {
 *   application_id = "my-app"
 *   application_name = "My Application"
 *   application_host = "localhost"
 *   enabled = true
 *
 *   http {
 *     base_url = "http://localhost:8081"
 *     username = "admin"
 *     password = "admin"
 *     target_path = "metrics"
 *     http_client_timeout_ms = 3000
 *   }
 *
 *   step_interval_ms = 10000
 *   min_batch_size = 1048576
 *   max_batch_size = 10485760
 *   max_send_interval_ms = 2000
 *   max_in_memory_bytes = 10485760
 *   max_on_disk_bytes = 1073741824
 *   retry_count = 3
 *   retry_interval_ms = 1000
 *   transformations = []
 *   partition_by = []
 * }
 * }</pre>
 */
public final class MetricsRegistryFactory {

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_micrometer");

    private MetricsRegistryFactory() {}

    /**
     * Create a MeterRegistry from configuration.
     *
     * @return A configured MeterRegistry
     */
    public static MeterRegistry create() {
        return createForwarder().getRegistry();
    }

    /**
     * Create a MicrometerForwarder from configuration.
     *
     * @return A configured and started MicrometerForwarder
     */
    public static MicrometerForwarder createForwarder() {
        MicrometerForwarderConfig forwarderConfig = createConfig();
        return MicrometerForwarder.createAndStart(forwarderConfig);
    }

    /**
     * Create a MicrometerForwarderConfig from the application configuration.
     *
     * @return A configured MicrometerForwarderConfig
     */
    public static MicrometerForwarderConfig createConfig() {
        Config http = config.getConfig("http");

        return MicrometerForwarderConfig.builder()
                .applicationId(config.getString("application_id"))
                .applicationName(config.getString("application_name"))
                .applicationHost(config.getString("application_host"))
                .baseUrl(http.getString("base_url"))
                .username(http.getString("username"))
                .password(http.getString("password"))
                .targetPath(http.getString("target_path"))
                .httpClientTimeout(Duration.ofMillis(http.getLong("http_client_timeout_ms")))
                .stepInterval(Duration.ofMillis(config.getLong("step_interval_ms")))
                .minBatchSize(config.getLong("min_batch_size"))
                .maxBatchSize(config.getLong("max_batch_size"))
                .maxSendInterval(Duration.ofMillis(config.getLong("max_send_interval_ms")))
                .maxInMemorySize(config.getLong("max_in_memory_bytes"))
                .maxOnDiskSize(config.getLong("max_on_disk_bytes"))
                .retryCount(config.getInt("retry_count"))
                .retryIntervalMillis(config.getLong("retry_interval_ms"))
                .transformations(config.getStringList("transformations"))
                .partitionBy(config.getStringList("partition_by"))
                .enabled(config.getBoolean("enabled"))
                .build();
    }
}
