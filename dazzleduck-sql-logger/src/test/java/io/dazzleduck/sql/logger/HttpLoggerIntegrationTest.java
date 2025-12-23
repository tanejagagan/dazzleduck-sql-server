package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpLoggerIntegrationTest {

    private Thread serverThread;
    String warehousePath = ConfigUtils.getWarehousePath(ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH));

    @BeforeAll
    void startServers() throws Exception {
        Config config = ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH);

        serverThread = new Thread(() -> {
            try {
                io.dazzleduck.sql.runtime.Main.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        waitForHttpServer(config);
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

            Path logFile = findFirstLogFile(warehousePath);

            TestUtils.isEqual(
                    """
                    select 'INFO'              as level,
                           'integration-test'  as logger,
                           'main'              as thread,
                           'Test 0'            as message,
                           'ap101'             as applicationId,
                           'MyApplication'     as applicationName,
                           'localhost'         as host
                    """, "select level, logger, thread, message, applicationId, applicationName, host\n" +
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
            logger.warn("Warning message");
            logger.error("Error message");
        } finally {
            logger.close();
        }

        // Verify all levels are persisted correctly
        Path logFile = findFirstLogFile(warehousePath);
        TestUtils.isEqual(
                "select 5 as count",
                "select count(*) as count from read_parquet('%s') where logger = 'multi-level-test'"
                        .formatted(logFile.toAbsolutePath())
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

        Path logFile = findFirstLogFile(warehousePath);
        // Verify stack trace is included in message
        TestUtils.isEqual(
                "select true as has_stacktrace",
                "select message like '%RuntimeException%' and message like '%Test exception%' as has_stacktrace " +
                        "from read_parquet('%s') where logger = 'exception-test'".formatted(logFile.toAbsolutePath())
        );
    }

    @Test
    void testHighVolumeBatching() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("batch-test");

        try {
            for (int i = 0; i < 1000; i++) {
                logger.info("Batch message {}", i);
            }
        } finally {
            logger.close();
        }

        Path logFile = findFirstLogFile(warehousePath);
        TestUtils.isEqual(
                "select 1000 as count",
                "select count(*) as count from read_parquet('%s') where logger = 'batch-test'"
                        .formatted(logFile.toAbsolutePath())
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

        // Should not throw exceptions
    }

    @Test
    void testParameterizedMessages() throws Exception {
        ArrowSimpleLogger logger = new ArrowSimpleLogger("param-test");

        try {
            logger.info("User {} logged in from {}", "john.doe", "192.168.1.1");
            logger.info("Order {} for customer {} total: {}", 12345, "jane", 99.99);
        } finally {
            logger.close();
        }

        String warehousePath = ConfigUtils.getWarehousePath(
                ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH)
        );

        Path logFile = findFirstLogFile(warehousePath);
        TestUtils.isEqual(
                "select 'User john.doe logged in from 192.168.1.1' as message",
                "select message from read_parquet('" + logFile.toAbsolutePath() +
                        "') where logger = 'param-test' and message like 'User%'"
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

        Path logFile = findFirstLogFile(warehousePath);
        TestUtils.isEqual(
                "select 100 as count",
                "select count(*) as count from read_parquet('%s') where logger = 'concurrent-test'"
                        .formatted(logFile.toAbsolutePath())
        );
    }

    @AfterAll
    void cleanupAndStop() throws Exception {
        cleanupWarehouse();
    }

    private void cleanupWarehouse() throws IOException {
        String warehousePath = ConfigUtils.getWarehousePath(
                ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH)
        );

        Path path = Path.of(warehousePath);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }

        Files.createDirectories(path);
    }

    private Path findFirstLogFile(String warehousePath) throws IOException {
        Path logsDir = Path.of(warehousePath);

        try (var stream = Files.list(logsDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No log file found in " + logsDir));
        }
    }

    private void waitForHttpServer(Config config) throws Exception {
        int port = 8081;

        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            try (var socket = new java.net.Socket("localhost", port)) {
                return; // port is open
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
    }
}