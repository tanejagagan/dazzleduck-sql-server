package io.dazzleduck.sql.logback;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;

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
        Config http = config.getConfig(ConfigUtils.HTTP_PREFIX);

        return LogForwarderConfig.builder()
                .applicationId(config.getString(ConfigUtils.APPLICATION_ID_KEY))
                .applicationName(config.getString(ConfigUtils.APPLICATION_NAME_KEY))
                .applicationHost(config.getString(ConfigUtils.APPLICATION_HOST_KEY))
                .baseUrl(http.getString(ConfigUtils.BASE_URL_KEY))
                .username(http.getString(ConfigUtils.USERNAME_KEY))
                .password(http.getString(ConfigUtils.PASSWORD_KEY))
                .targetPath(http.getString(ConfigUtils.TARGET_PATH_KEY))
                .httpClientTimeout(Duration.ofMillis(http.getLong(ConfigUtils.HTTP_CLIENT_TIMEOUT_MS_KEY)))
                .maxBufferSize(config.getInt(ConfigUtils.MAX_BUFFER_SIZE_KEY))
                .pollInterval(Duration.ofMillis(config.getLong(ConfigUtils.POLL_INTERVAL_MS_KEY)))
                .minBatchSize(config.getLong(ConfigUtils.MIN_BATCH_SIZE_KEY))
                .maxBatchSize(config.getLong(ConfigUtils.MAX_BATCH_SIZE_KEY))
                .maxSendInterval(Duration.ofMillis(config.getLong(ConfigUtils.MAX_SEND_INTERVAL_MS_KEY)))
                .maxInMemorySize(config.getLong(ConfigUtils.MAX_IN_MEMORY_BYTES_KEY))
                .maxOnDiskSize(config.getLong(ConfigUtils.MAX_ON_DISK_BYTES_KEY))
                .retryCount(config.getInt(ConfigUtils.RETRY_COUNT_KEY))
                .retryIntervalMillis(config.getLong(ConfigUtils.RETRY_INTERVAL_MS_KEY))
                .transformations(config.getStringList(ConfigUtils.TRANSFORMATIONS_KEY))
                .partitionBy(config.getStringList(ConfigUtils.PARTITION_BY_KEY))
                .enabled(config.getBoolean(ConfigUtils.ENABLED_KEY))
                .build();
    }
}
