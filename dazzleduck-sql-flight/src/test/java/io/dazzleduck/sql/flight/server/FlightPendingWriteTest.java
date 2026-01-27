package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.dazzleduck.sql.flight.stream.ArrowStreamReaderWrapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for verifying that PendingWriteExceededException returns correct
 * RESOURCE_EXHAUSTED status code in Flight SQL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlightPendingWriteTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "password";
    private static final String CATALOG = "memory";
    private static final String SCHEMA_NAME = "main";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private Path warehousePath;
    private Location serverLocation;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        serverLocation = FlightTestUtils.findNextLocation();
        warehousePath = Files.createTempDirectory("pending-write-test");

        // Install arrow extension
        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow"
        });

        // Create server with max_pending_write set to 1 byte
        // This ensures any ingestion will exceed the limit
        IngestionConfig lowLimitConfig = new IngestionConfig(
                1024 * 1024,  // minBucketSize
                2048,         // maxBatches
                1L,           // maxPendingWrite - set to 1 byte to trigger limit
                Duration.ofSeconds(2)
        );

        String producerId = UUID.randomUUID().toString();
        var producer = new DuckDBFlightSqlProducer(
                serverLocation,
                producerId,
                "test-secret",
                allocator,
                warehousePath.toString(),
                AccessMode.COMPLETE,
                DuckDBFlightSqlProducer.newTempDir(),
                new NOOPIngestionTaskFactoryProvider(warehousePath + File.separator + "ingestion").getIngestionTaskFactory(),
                Executors.newSingleThreadScheduledExecutor(),
                Duration.ofMinutes(2),
                Clock.systemDefaultZone(),
                new MicroMeterFlightRecorder(new SimpleMeterRegistry(), producerId),
                lowLimitConfig
        );

        server = FlightServer.builder(allocator, serverLocation, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();

        // Create client
        client = new FlightSqlClient(FlightClient.builder(allocator, serverLocation)
                .intercept(AuthUtils.createClientMiddlewareFactory(USER, PASSWORD,
                        Map.of(Headers.HEADER_DATABASE, CATALOG, Headers.HEADER_SCHEMA, SCHEMA_NAME)))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        if (warehousePath != null) {
            deleteDirectory(warehousePath.toFile());
        }
    }

    @Test
    void testIngestionRejectedWhenPendingWriteExceeded() throws Exception {
        String testPath = "pending_write_test";
        Files.createDirectories(warehousePath.resolve(testPath));

        String query = "SELECT * FROM generate_series(10)";

        try (DuckDBConnection conn = ConnectionPool.getConnection();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator, query, 1000)) {

            var streamReader = new ArrowStreamReaderWrapper(reader, allocator);
            var ingestOptions = new FlightSqlClient.ExecuteIngestOptions("",
                    FlightSql.CommandStatementIngest.TableDefinitionOptions.newBuilder().build(),
                    false, CATALOG, SCHEMA_NAME,
                    Map.of(Headers.QUERY_PARAMETER_INGESTION_QUEUE, testPath));

            // Should throw FlightRuntimeException with RESOURCE_EXHAUSTED status
            FlightRuntimeException exception = assertThrows(FlightRuntimeException.class, () -> {
                client.executeIngest(streamReader, ingestOptions);
            });

            // Verify the status code is RESOURCE_EXHAUSTED
            assertEquals(FlightStatusCode.RESOURCE_EXHAUSTED, exception.status().code(),
                    "Expected RESOURCE_EXHAUSTED status when pending write limit exceeded");

            // Verify the error message mentions pending write limit
            assertTrue(exception.getMessage().contains("Pending write limit exceeded"),
                    "Error message should mention pending write limit exceeded. Actual: " + exception.getMessage());
        }
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
