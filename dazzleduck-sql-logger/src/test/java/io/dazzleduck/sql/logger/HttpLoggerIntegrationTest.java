package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpLoggerIntegrationTest {

    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    private static final String CONFIG_HTTP_TARGET_PATH = config.getString("http.target_path");
    // ArrowSimpleLogger reads base_url from config (e.g., "http://localhost:8081")
    // Extract port from base_url to ensure server matches
    private static final int LOGGER_CONFIG_HTTP_PORT = extractPortFromBaseUrl(config.getString("http.base_url"));

    private SharedTestServer server;
    private String warehousePath;

    private static int extractPortFromBaseUrl(String baseUrl) {
        // baseUrl format: "http://localhost:8081"
        int lastColon = baseUrl.lastIndexOf(':');
        if (lastColon > 0) {
            return Integer.parseInt(baseUrl.substring(lastColon + 1));
        }
        return 8081; // default
    }

    @BeforeAll
    void startServer() throws Exception {
        server = new SharedTestServer();
        server.startWithPorts(LOGGER_CONFIG_HTTP_PORT, 0);
        warehousePath = server.getWarehousePath();
        Files.createDirectories(Path.of(warehousePath + "/" + CONFIG_HTTP_TARGET_PATH));
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testLoggerCanPostLogs() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("integration-test");

        try {
            for (int i = 0; i < 10; i++) {
                logger.info("Test {}", i);
            }
        } finally {
            logger.close();
        }

        Path logFile = findFirstLogFile(warehousePath,"integration-test");

        TestUtils.isEqual(
                """
                select 'INFO'              as level,
                       'integration-test'  as logger,
                       'main'              as thread,
                       'Test 0'            as message,
                       'ap101'             as application_id,
                       'MyApplication'     as application_name,
                       'localhost'         as application_host
                """, "select level, logger, thread, message, application_id, application_name, application_host\n" +
                        "from read_parquet('%s') where message = 'Test 0'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testMultipleLogLevels() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("multi-level-test");

        try {
            logger.trace("Trace message");
            logger.debug("Debug message");
            logger.info("Info message");
        } finally {
            logger.close();
        }

        // Verify all levels are persisted correctly
        Path logFile = findFirstLogFile(warehousePath,"multi-level-test");
        TestUtils.isEqual("select 3 as count", "select count(*) as count from read_parquet('%s') where logger = 'multi-level-test'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testExceptionLogging() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("exception-test");

        try {
            Exception ex = new RuntimeException("Test exception");
            logger.error("Error occurred", ex);
        } finally {
            logger.close();
        }

        Path logFile = findFirstLogFile(warehousePath,"exception-test");
        // Verify stack trace is included in message
        TestUtils.isEqual(
                "select true as has_stacktrace",
                "select message like '%RuntimeException%' and message like '%Test exception%' as has_stacktrace " +
                        "from read_parquet('%s') where logger = 'exception-test'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testEmptyAndNullMessages() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("empty-test");

        try {
            logger.info("");
            logger.info(null);
        } finally {
            logger.close();
        }
    }
    @Test
    void testParameterizedMessages() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("param-test");

        try {
            logger.info("User {} logged in from {}", "john.doe", "192.168.1.1");
        } finally {
            logger.close();
        }

        Path logFile = findFirstLogFile(warehousePath,"param-test");
        TestUtils.isEqual(
                "select 'User john.doe logged in from 192.168.1.1' as message",
                """
                 select trim(message) as message from read_parquet('%s') where logger = 'param-test'
                """.formatted(logFile.toAbsolutePath())
        );
    }
    @Test
    void testConcurrentLogging() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("concurrent-test");

        try {
            Thread[] threads = new Thread[5];
            for (int t = 0; t < threads.length; t++) {
                final int threadNum = t;
                threads[t] = new Thread(() -> {
                    for (int i = 0; i < 20; i++) {
                        logger.info("Thread {} message {}", threadNum, i);
                    }
                });
                threads[t].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            logger.close();
        }

        Path logFile = findFirstLogFile(warehousePath,"concurrent-test");
        TestUtils.isEqual(
                "select 100 as count",
                "select count(*) as count from read_parquet('%s') where logger = 'concurrent-test'"
                        .formatted(logFile.toAbsolutePath())
        );
    }

    private Path findFirstLogFile(String warehousePath, String loggerName) throws Exception {
        try (var stream = Files.walk(Path.of(warehousePath))) {
            for (Path file : stream.filter(p -> p.toString().endsWith(".parquet")).toList()) {
                try {
                    TestUtils.isEqual(
                            "select 1 as ok",
                            "select 1 as ok from read_parquet('%s') where logger = '%s' limit 1"
                                    .formatted(file.toAbsolutePath(), loggerName)
                    );
                    return file; // this file contains the logger
                } catch (AssertionError ignored) {
                    // not the right file, keep scanning
                }
            }
        }
        throw new IllegalStateException(
                "No parquet file found for logger=" + loggerName + " in " + warehousePath);
    }

}