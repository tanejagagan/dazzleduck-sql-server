package io.dazzleduck.sql.logger.tailing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.common.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main entry point for log tail to Arrow processor.
 */
public final class LogProcessorMain {

    private static final Logger logger = LoggerFactory.getLogger(LogProcessorMain.class);

    // Logger-specific config keys
    private static final String LOG_DIRECTORY_KEY = "log_directory";
    private static final String LOG_FILE_PATTERN_KEY = "log_file_pattern";

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    // Application config
    private static final String CONFIG_APPLICATION_ID = config.getString(ConfigUtils.APPLICATION_ID_KEY);
    private static final String CONFIG_APPLICATION_NAME = config.getString(ConfigUtils.APPLICATION_NAME_KEY);
    private static final String CONFIG_APPLICATION_HOST = config.getString(ConfigUtils.APPLICATION_HOST_KEY);
    // Log monitoring config
    private static final String CONFIG_LOG_DIRECTORY = config.getString(LOG_DIRECTORY_KEY);
    private static final String CONFIG_LOG_FILE_PATTERN = config.getString(LOG_FILE_PATTERN_KEY);
    private static final long CONFIG_POLL_INTERVAL_MS = config.getLong(ConfigUtils.POLL_INTERVAL_MS_KEY);
    // HTTP config
    private static final Config httpConfig = config.getConfig(ConfigUtils.HTTP_PREFIX);
    private static final String CONFIG_HTTP_BASE_URL = httpConfig.getString(ConfigUtils.BASE_URL_KEY);
    private static final String CONFIG_HTTP_USERNAME = httpConfig.getString(ConfigUtils.USERNAME_KEY);
    private static final String CONFIG_HTTP_PASSWORD = httpConfig.getString(ConfigUtils.PASSWORD_KEY);
    private static final String CONFIG_HTTP_TARGET_PATH = httpConfig.getString(ConfigUtils.TARGET_PATH_KEY);
    private static final long CONFIG_HTTP_TIMEOUT_MS = httpConfig.getLong(ConfigUtils.HTTP_CLIENT_TIMEOUT_MS_KEY);
    // Batch and memory config
    private static final long CONFIG_MIN_BATCH_SIZE = config.getLong(ConfigUtils.MIN_BATCH_SIZE_KEY);
    private static final long CONFIG_MAX_BATCH_SIZE = config.getLong(ConfigUtils.MAX_BATCH_SIZE_KEY);
    private static final long CONFIG_MAX_SEND_INTERVAL_MS = config.getLong(ConfigUtils.MAX_SEND_INTERVAL_MS_KEY);
    private static final long CONFIG_MAX_IN_MEMORY_BYTES = config.getLong(ConfigUtils.MAX_IN_MEMORY_BYTES_KEY);
    private static final long CONFIG_MAX_ON_DISK_BYTES = config.getLong(ConfigUtils.MAX_ON_DISK_BYTES_KEY);

    public static void main(String[] args) throws InterruptedException {
        // Create converter to get schema
        JsonToArrowConverter converter = new JsonToArrowConverter(
                CONFIG_APPLICATION_ID,
                CONFIG_APPLICATION_NAME,
                CONFIG_APPLICATION_HOST
        );

        // Create HttpSender with configuration
        HttpProducer httpSender = new HttpProducer(
                converter.getSchema(),
                CONFIG_HTTP_BASE_URL,
                CONFIG_HTTP_USERNAME,
                CONFIG_HTTP_PASSWORD,
                CONFIG_HTTP_TARGET_PATH,
                Duration.ofMillis(CONFIG_HTTP_TIMEOUT_MS),
                CONFIG_MIN_BATCH_SIZE,
                CONFIG_MAX_BATCH_SIZE,
                Duration.ofMillis(CONFIG_MAX_SEND_INTERVAL_MS),
                config.getInt(ConfigUtils.RETRY_COUNT_KEY),
                config.getLong(ConfigUtils.RETRY_INTERVAL_MS_KEY),
                config.getStringList(ConfigUtils.TRANSFORMATIONS_KEY),
                config.getStringList(ConfigUtils.PARTITION_BY_KEY),
                CONFIG_MAX_IN_MEMORY_BYTES,
                CONFIG_MAX_ON_DISK_BYTES
        );
        // Create and start processor with directory monitoring
        LogTailToArrowProcessor processor = new LogTailToArrowProcessor(
                CONFIG_LOG_DIRECTORY,
                CONFIG_LOG_FILE_PATTERN,
                converter,
                httpSender,
                CONFIG_POLL_INTERVAL_MS
        );

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            processor.close();
        }));

        // Start processing
        processor.start();

        logger.info("Processor started. Monitoring directory for log files...");

        // Keep main thread alive
        while (processor.isRunning()) {
            Thread.sleep(1000);
        }
    }
}