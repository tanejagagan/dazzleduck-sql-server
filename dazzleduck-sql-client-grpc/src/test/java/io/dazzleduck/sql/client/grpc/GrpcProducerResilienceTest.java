package io.dazzleduck.sql.client.grpc;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Resilience tests for GrpcArrowProducer.
 *
 * Test flow:
 * 1. Create producer on fixed port BEFORE server starts
 * 2. Producer sends events for 6 seconds (one event every 10ms = 600 events)
 * 3. Server starts in background after 0.5 seconds
 * 4. Server runs until test ends
 * 5. After producer closes, validate all distinct events are received
 */
@Tag("slow")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class GrpcProducerResilienceTest {

    private static final Logger logger = LoggerFactory.getLogger(GrpcProducerResilienceTest.class);
    private static final String HOST = "localhost";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin";

    private static final int TEST_DURATION_MS = 6_000;
    private static final int EVENT_INTERVAL_MS = 10;
    private static final int SERVER_START_DELAY_MS = 500;

    private RootAllocator allocator;

    @BeforeAll
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterAll
    void teardown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    void testGrpcArrowProducerResilience() throws Exception {
        int fixedFlightPort = findAvailablePort();
        int fixedHttpPort = findAvailablePort();
        Path warehousePath = Files.createTempDirectory("grpc-resilience-warehouse");
        Path ingestionPath = Files.createTempDirectory("grpc-resilience-ingestion");
        String testPath = "grpc-data";
        Files.createDirectories(ingestionPath.resolve(testPath));

        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        int expectedEvents = TEST_DURATION_MS / EVENT_INTERVAL_MS;

        // Server starts late and stays up until end (no cycling to avoid port reuse issues)
        SharedTestServer server = new SharedTestServer();

        // Start server in background after delay
        Thread serverThread = new Thread(() -> {
            try {
                logger.info("Server thread: waiting {}ms before start...", SERVER_START_DELAY_MS);
                Thread.sleep(SERVER_START_DELAY_MS);

                logger.info("Starting server on ports HTTP:{}, Flight:{}", fixedHttpPort, fixedFlightPort);
                server.startWithPorts(fixedHttpPort, fixedFlightPort,
                        "warehouse=" + warehousePath,
                        "ingestion_task_factory_provider.ingestion_path=" + ingestionPath,
                        "ingestion.max_delay_ms=0");
                logger.info("Server started successfully");

            } catch (InterruptedException e) {
                logger.info("Server thread interrupted before start");
            } catch (Exception e) {
                logger.error("Server start failed", e);
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        try {
            // Create producer BEFORE server starts and send events
            logger.info("Creating GrpcArrowProducer on port {} (server not yet started)", fixedFlightPort);
            try (GrpcArrowProducer producer = new GrpcArrowProducer(
                    schema,
                    1024,
                    2048,
                    Duration.ofMillis(100),
                    Clock.systemUTC(),
                    2000, // retryCount - high for resilience during initial outage
                    100,  // retryIntervalMillis
                    List.of(),
                    List.of(),
                    10_000_000,  // larger buffer
                    50_000_000,  // larger disk buffer
                    allocator,
                    Location.forGrpcInsecure(HOST, fixedFlightPort),
                    USER,
                    PASSWORD,
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, testPath),
                    Duration.ofSeconds(60)
            )) {
                logger.info("Sending {} events over {}ms...", expectedEvents, TEST_DURATION_MS);
                long startTime = System.currentTimeMillis();

                for (int i = 0; i < expectedEvents; i++) {
                    producer.addRow(new JavaRow(new Object[]{(long) i}));
                    Thread.sleep(EVENT_INTERVAL_MS);
                }

                long elapsed = System.currentTimeMillis() - startTime;
                logger.info("Finished sending {} events in {}ms", expectedEvents, elapsed);

                // Wait for server thread to complete startup if it hasn't already
                serverThread.join(5000);

                // Wait for producer to flush remaining data
                logger.info("Waiting for producer to flush data...");
                Thread.sleep(3000);
            }
            // Producer closed, all data should be flushed

            // Wait for server to finish writing
            Thread.sleep(2000);

            // Verify all distinct events received
            logger.info("Verifying data...");
            var actualQuery = String.format("SELECT DISTINCT id FROM read_parquet('%s/%s/*.parquet') ORDER BY id", ingestionPath, testPath);
            var expectedQuery = String.format("SELECT * FROM generate_series(0, %d) AS t(id) ORDER BY id", expectedEvents - 1);
            TestUtils.isEqual(expectedQuery, actualQuery);

            logger.info("GrpcArrowProducer resilience test passed - all {} distinct events received", expectedEvents);

        } finally {
            server.close();
            cleanupDirectory(warehousePath);
            cleanupDirectory(ingestionPath);
        }
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void cleanupDirectory(Path path) {
        try {
            if (Files.exists(path)) {
                Files.walk(path)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup directory: {}", path, e);
        }
    }
}
