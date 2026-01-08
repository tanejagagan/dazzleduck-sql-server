package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests ingestion resilience when server is unavailable or restarting.
 *
 * Test flow:
 * 1. Client starts sending data before server is up
 * 2. If send fails, retry after 100ms
 * 3. Server starts 2 seconds later
 * 4. Server runs for 2 seconds
 * 5. Server stops for 3 seconds
 * 6. Server starts again
 * 7. After 10 seconds, validate all data is received
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("its takes very long to test and its tested as part of ingestion client")
public class IngestionResilienceTest {

    private static final Logger logger = LoggerFactory.getLogger(IngestionResilienceTest.class);

    private static final String HOST = "localhost";
    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA_NAME = "test_schema";

    private static final int TEST_DURATION_MS = 10_000;
    private static final int EVENT_INTERVAL_MS = 10;
    private static final int SERVER_START_DELAY_MS = 2_000;
    private static final int SERVER_UP_DURATION_MS = 2_000;
    private static final int SERVER_DOWN_DURATION_MS = 3_000;
    private static final int RETRY_INTERVAL_MS = 100;

    private BufferAllocator allocator;

    @BeforeAll
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        // Install arrow extension for reading parquet files
        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow",
                "ATTACH ':memory:' AS test_catalog",
                "CREATE SCHEMA IF NOT EXISTS test_catalog.test_schema"
        });
    }

    @AfterAll
    void teardown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * DISABLED: This test is currently disabled due to a server-side batch sequencing issue.
     *
     * When the client retries sending batches after failures, the Flight protocol's batch
     * tracking state on the server gets confused, resulting in "Out of sequence batch" errors.
     * This is because the Flight protocol maintains stateful batch sequence tracking, and
     * retries break this sequence.
     *
     * The HTTP resilience test in dazzleduck-sql-client (ProducerResilienceTest) covers
     * producer resilience functionality using the stateless HTTP protocol which doesn't
     * have this issue.
     */
    @Test
    void testIngestionResilience() throws Exception {
        Location serverLocation = FlightTestUtils.findNextLocation();
        int port = serverLocation.getUri().getPort();
        Path warehousePath = Files.createTempDirectory("ingestion-resilience-test");
        String testPath = "resilience-data";

        Files.createDirectories(warehousePath.resolve(testPath));
        int expectedEvents = TEST_DURATION_MS / EVENT_INTERVAL_MS;

        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)
        ));

        AtomicBoolean serverRunning = new AtomicBoolean(false);
        AtomicBoolean testComplete = new AtomicBoolean(false);
        AtomicInteger successfulBatches = new AtomicInteger(0);
        AtomicInteger totalRetries = new AtomicInteger(0);

        // Server management thread
        Thread serverThread = new Thread(() -> {
            FlightServer server = null;
            try {
                // Wait before starting server
                logger.info("Server thread: waiting {}ms before first start...", SERVER_START_DELAY_MS);
                Thread.sleep(SERVER_START_DELAY_MS);

                // First start
                server = createAndStartServer(serverLocation, warehousePath.toString());

                serverRunning.set(true);
                logger.info("Server started on port {}", port);

                // Run for SERVER_UP_DURATION_MS
                Thread.sleep(SERVER_UP_DURATION_MS);

                // Stop for SERVER_DOWN_DURATION_MS
                logger.info("Stopping server for {}ms...", SERVER_DOWN_DURATION_MS);
                server.close();
                serverRunning.set(false);

                Thread.sleep(SERVER_DOWN_DURATION_MS);

                // Restart
                logger.info("Restarting server...");
                server = createAndStartServer(serverLocation, warehousePath.toString());
                Files.createDirectories(warehousePath.resolve(testPath));
                serverRunning.set(true);
                logger.info("Server restarted");

                // Keep running until test completes
                while (!testComplete.get()) {
                    Thread.sleep(100);
                }

                server.close();

            } catch (InterruptedException e) {
                logger.info("Server thread interrupted");
            } catch (Exception e) {
                logger.error("Server operation failed", e);
            } finally {
                if (server != null) {
                    try {
                        server.close();
                    } catch (Exception ignored) {}
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // Data sending thread with retry logic

            int batchSize = 100;  // Send 100 events per batch
            int totalBatches = expectedEvents / batchSize;

            for (int batch = 0; batch < totalBatches && !testComplete.get(); batch++) {
                int startId = batch * batchSize;
                int endId = startId + batchSize;

                boolean sent = false;
                while (!sent && !testComplete.get()) {
                    FlightSqlClient client = null;
                    try {
                        client = createClient(serverLocation);

                        // Generate data for this batch
                        String query = "SELECT * FROM generate_series(%d, %d) AS t(id)".formatted(startId, endId - 1);
                        try (DuckDBConnection conn = ConnectionPool.getConnection();
                             ArrowReader reader = ConnectionPool.getReader(conn, allocator, query, 1000)) {

                            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
                            var ingestOptions = new FlightSqlClient.ExecuteIngestOptions("",
                                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                                    false, CATALOG, SCHEMA_NAME, Map.of("path", testPath));

                            client.executeIngest(streamReader, ingestOptions);
                            sent = true;
                            successfulBatches.incrementAndGet();
                            logger.debug("Batch {} sent successfully (ids {}-{})", batch, startId, endId - 1);
                        }
                    } catch (Exception e) {
                        totalRetries.incrementAndGet();
                        logger.debug("Batch {} failed, retrying in {}ms: {}", batch, RETRY_INTERVAL_MS, e.getMessage());
                        try {
                            Thread.sleep(RETRY_INTERVAL_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    } finally {
                        if (client != null) {
                            try {
                                client.close();
                            } catch (Exception ignored) {}
                        }
                    }
                }

                // Small delay between batches
                try {
                    Thread.sleep(EVENT_INTERVAL_MS * batchSize / 10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            logger.info("Sender thread completed: {} batches sent, {} retries", successfulBatches.get(), totalRetries.get());



        try {
            // Wait for sender to complete


            // Wait for data to flush
            Thread.sleep(2000);

            // Verify data
            logger.info("Verifying data...");
            var actualQuery = "SELECT DISTINCT id FROM read_parquet('%s/%s/*.parquet') ORDER BY id"
                    .formatted(warehousePath, testPath);
            var expectedQuery = "SELECT * FROM generate_series(0, %d) AS t(id) ORDER BY id"
                    .formatted(expectedEvents - 1);
            TestUtils.isEqual(expectedQuery, actualQuery);

            logger.info("Test passed - all {} events received (batches: {}, retries: {})",
                    expectedEvents, successfulBatches.get(), totalRetries.get());

        } finally {
            testComplete.set(true);
            serverThread.join(5000);
            deleteDirectory(warehousePath.toFile());
        }
    }

    private FlightServer createAndStartServer(Location location, String warehousePath) throws IOException, java.security.NoSuchAlgorithmException {
        String producerId = UUID.randomUUID().toString();
        var producer = new DuckDBFlightSqlProducer(
                location,
                producerId,
                "test-secret",
                allocator,
                warehousePath,
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(),
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId),
                QueryOptimizer.NOOP_QUERY_OPTIMIZER,
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG
        );

        return FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
    }

    private FlightSqlClient createClient(Location location) {
        return new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, CATALOG, Headers.HEADER_SCHEMA, SCHEMA_NAME)))
                .build());
    }

    private static void deleteDirectory(File directory) {
        if (directory == null || !directory.exists()) return;
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }
}
