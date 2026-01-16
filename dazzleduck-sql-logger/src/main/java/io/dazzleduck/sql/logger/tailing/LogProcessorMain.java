package io.dazzleduck.sql.logger.tailing;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpArrowProducer;
import io.dazzleduck.sql.common.ConfigConstants;
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
    // Log monitoring config
    private static final String CONFIG_LOG_DIRECTORY = config.getString(LOG_DIRECTORY_KEY);
    private static final String CONFIG_LOG_FILE_PATTERN = config.getString(LOG_FILE_PATTERN_KEY);
    private static final long CONFIG_POLL_INTERVAL_MS = config.getLong(ConfigConstants.POLL_INTERVAL_MS_KEY);
    // HTTP config
    private static final Config httpConfig = config.getConfig(ConfigConstants.HTTP_PREFIX);
    private static final String CONFIG_HTTP_BASE_URL = httpConfig.getString(ConfigConstants.BASE_URL_KEY);
    private static final String CONFIG_HTTP_USERNAME = httpConfig.getString(ConfigConstants.USERNAME_KEY);
    private static final String CONFIG_HTTP_PASSWORD = httpConfig.getString(ConfigConstants.PASSWORD_KEY);
    private static final String CONFIG_HTTP_TARGET_PATH = httpConfig.getString(ConfigConstants.TARGET_PATH_KEY);
    private static final long CONFIG_HTTP_TIMEOUT_MS = httpConfig.getLong(ConfigConstants.HTTP_CLIENT_TIMEOUT_MS_KEY);
    // Batch and memory config
    private static final long CONFIG_MIN_BATCH_SIZE = config.getLong(ConfigConstants.MIN_BATCH_SIZE_KEY);
    private static final long CONFIG_MAX_BATCH_SIZE = config.getLong(ConfigConstants.MAX_BATCH_SIZE_KEY);
    private static final long CONFIG_MAX_SEND_INTERVAL_MS = config.getLong(ConfigConstants.MAX_SEND_INTERVAL_MS_KEY);
    private static final long CONFIG_MAX_IN_MEMORY_BYTES = config.getLong(ConfigConstants.MAX_IN_MEMORY_BYTES_KEY);
    private static final long CONFIG_MAX_ON_DISK_BYTES = config.getLong(ConfigConstants.MAX_ON_DISK_BYTES_KEY);

    public static void main(String[] args) throws InterruptedException {
        // Create converter to get schema
        JsonToArrowConverter converter = new JsonToArrowConverter();

        // Create HttpSender with configuration
        HttpArrowProducer httpSender = new HttpArrowProducer(
                converter.getSchema(),
                CONFIG_HTTP_BASE_URL,
                CONFIG_HTTP_USERNAME,
                CONFIG_HTTP_PASSWORD,
                CONFIG_HTTP_TARGET_PATH,
                Duration.ofMillis(CONFIG_HTTP_TIMEOUT_MS),
                CONFIG_MIN_BATCH_SIZE,
                CONFIG_MAX_BATCH_SIZE,
                Duration.ofMillis(CONFIG_MAX_SEND_INTERVAL_MS),
                config.getInt(ConfigConstants.RETRY_COUNT_KEY),
                config.getLong(ConfigConstants.RETRY_INTERVAL_MS_KEY),
                config.getStringList(ConfigConstants.PROJECTIONS_KEY),
                config.getStringList(ConfigConstants.PARTITION_BY_KEY),
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