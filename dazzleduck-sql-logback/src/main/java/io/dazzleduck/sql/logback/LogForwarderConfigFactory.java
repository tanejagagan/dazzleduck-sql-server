package io.dazzleduck.sql.logback;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;

import java.time.Duration;

/**
 * Factory for creating LogForwarderConfig instances configured from application.conf.
 *
 * <p>Expected configuration format in application.conf:</p>
 * <pre>{@code
 * dazzleduck_logback {
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
 *   projections = ["*", "CAST (timestamp AS date) AS date"]
 *   partition_by = [date]
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
        Config http = config.getConfig(ConfigConstants.HTTP_PREFIX);

        return LogForwarderConfig.builder()
                .baseUrl(http.getString(ConfigConstants.BASE_URL_KEY))
                .username(http.getString(ConfigConstants.USERNAME_KEY))
                .password(http.getString(ConfigConstants.PASSWORD_KEY))
                .targetPath(http.getString(ConfigConstants.TARGET_PATH_KEY))
                .httpClientTimeout(Duration.ofMillis(http.getLong(ConfigConstants.HTTP_CLIENT_TIMEOUT_MS_KEY)))
                .maxBufferSize(config.getInt(ConfigConstants.MAX_BUFFER_SIZE_KEY))
                .pollInterval(Duration.ofMillis(config.getLong(ConfigConstants.POLL_INTERVAL_MS_KEY)))
                .minBatchSize(config.getLong(ConfigConstants.MIN_BATCH_SIZE_KEY))
                .maxBatchSize(config.getLong(ConfigConstants.MAX_BATCH_SIZE_KEY))
                .maxSendInterval(Duration.ofMillis(config.getLong(ConfigConstants.MAX_SEND_INTERVAL_MS_KEY)))
                .maxInMemorySize(config.getLong(ConfigConstants.MAX_IN_MEMORY_BYTES_KEY))
                .maxOnDiskSize(config.getLong(ConfigConstants.MAX_ON_DISK_BYTES_KEY))
                .retryCount(config.getInt(ConfigConstants.RETRY_COUNT_KEY))
                .retryIntervalMillis(config.getLong(ConfigConstants.RETRY_INTERVAL_MS_KEY))
                .project(config.getStringList(ConfigConstants.PROJECT_KEY))
                .partitionBy(config.getStringList(ConfigConstants.PARTITION_BY_KEY))
                .enabled(config.getBoolean(ConfigConstants.ENABLED_KEY))
                .build();
    }
}
