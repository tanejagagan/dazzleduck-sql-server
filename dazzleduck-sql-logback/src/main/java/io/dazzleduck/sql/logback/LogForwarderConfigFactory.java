package io.dazzleduck.sql.logback;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;

/**
 * Factory for creating LogForwarderConfig instances configured from application.conf.
 *
 * <p>Expected configuration format in application.conf:</p>
 * <pre>{@code
 * dazzleduck_logback {
 *   application_id = "my-app"
 *   application_name = "My Application"
 *   application_host = "localhost"
 *   enabled = true
 *
 *   max_buffer_size = 10000
 *   poll_interval_ms = 5000
 *
 *   http {
 *     base_url = "http://localhost:8081"
 *     username = "admin"
 *     password = "admin"
 *     target_path = "logs"
 *     http_client_timeout_ms = 3000
 *   }
 *
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
public final class LogForwarderConfigFactory {

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logback");

    private LogForwarderConfigFactory() {}

    /**
     * Create a LogForwarder from configuration.
     *
     * @return A configured and started LogForwarder
     */
    public static LogForwarder createForwarder() {
        LogForwarderConfig forwarderConfig = createConfig();
        return LogForwarder.createAndStart(forwarderConfig);
    }

    /**
     * Create a LogForwarderConfig from the application configuration.
     *
     * @return A configured LogForwarderConfig
     */
    public static LogForwarderConfig createConfig() {
        Config http = config.getConfig("http");

        return LogForwarderConfig.builder()
                .applicationId(config.getString("application_id"))
                .applicationName(config.getString("application_name"))
                .applicationHost(config.getString("application_host"))
                .baseUrl(http.getString("base_url"))
                .username(http.getString("username"))
                .password(http.getString("password"))
                .targetPath(http.getString("target_path"))
                .httpClientTimeout(Duration.ofMillis(http.getLong("http_client_timeout_ms")))
                .maxBufferSize(config.getInt("max_buffer_size"))
                .pollInterval(Duration.ofMillis(config.getLong("poll_interval_ms")))
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
