package io.dazzleduck.sql.client;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;


import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.CONCURRENT)
public class HttpArrowProducerTest {

    private static SharedTestServer server;
    private static String warehouse;
    private static String baseUrl;
    private static Schema schema;

    @BeforeAll
    static void setup() throws Exception {
        server = new SharedTestServer();
        server.start();
        warehouse = server.getWarehousePath();
        baseUrl = server.getHttpBaseUrl();
        schema = new Schema(java.util.List.of(new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    @AfterAll
    static void teardown() {
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    void setupEach() {
        org.awaitility.Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
        org.awaitility.Awaitility.setDefaultTimeout(2, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardownEach() {
        org.awaitility.Awaitility.reset();
    }

    private HttpArrowProducer newSender(String file, Duration timeout) {
        return new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                file,
                timeout,
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100,
                100_000,
                Clock.systemDefaultZone()
        );
    }

    private byte[] arrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection conn = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(conn, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return baos.toByteArray();
        }
    }

    private void verifyFile(String path, int maxValue) throws Exception {
        var actualQuery = "SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series".formatted(warehouse, path);
        var expectedQuery = "SELECT * FROM generate_series(%d) ORDER BY generate_series".formatted(maxValue);
        TestUtils.isEqual(expectedQuery, actualQuery);
    }

    @Test
    void testAsyncIngestionSingleBatch() throws Exception {
        String file = "async-single-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));
        try (HttpArrowProducer sender = newSender(file, Duration.ofSeconds(10))) {
            sender.enqueue(arrowBytes("select * from generate_series(4)"));
        }

        // Verify after close() has flushed all data
        verifyFile(file, 4);
    }

    @Test
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {
        String file = "overwrite-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));

        try (HttpArrowProducer overwriteSender = new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                file,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100_000,
                500_000)) {

            overwriteSender.enqueue(arrowBytes("select * from generate_series(1)"));
            overwriteSender.enqueue(arrowBytes("select * from generate_series(2)"));
        }

        // Verify after close() has flushed all data - combined results from both enqueues
        await()
                .pollInterval(6, TimeUnit.MILLISECONDS)
                .atMost(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    var actualQuery = "SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series".formatted(warehouse, file);
                    var expectedQuery = "SELECT * FROM (VALUES (0), (0), (1), (1), (2)) AS t(generate_series)";
                    TestUtils.isEqual(expectedQuery, actualQuery);
                });
    }

    @Test
    void testConcurrentEnqueues() throws Exception {
        String file = "concurrent-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));
        CountDownLatch latch = new CountDownLatch(5);
        AtomicInteger errors = new AtomicInteger(0);

        try (HttpArrowProducer concurrentEnqueues = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                final int index = i;
                new Thread(() -> {
                    try {
                        concurrentEnqueues.enqueue(arrowBytes("select * from generate_series(" + (index * 10) + ")"));
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertEquals(0, errors.get());
            // Verify that all concurrent enqueues completed - total rows = 1 + 11 + 21 + 31 + 41 = 105
            await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                var actualQuery = "SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series".formatted(warehouse, file);
                var expectedQuery = "SELECT * FROM (SELECT * FROM generate_series(0) UNION ALL SELECT * FROM generate_series(10) UNION ALL SELECT * FROM generate_series(20) UNION ALL SELECT * FROM generate_series(30) UNION ALL SELECT * FROM generate_series(40)) ORDER BY generate_series";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testJWTTokenReuse() throws Exception {
        String file = "jwt-reuse-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));

        // Multiple requests should reuse the same token
        try (HttpArrowProducer reuseSender = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                reuseSender.enqueue(arrowBytes("select " + i + " as val"));
            }

            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                var actualQuery = "SELECT val FROM read_parquet('%s/%s/*.parquet') ORDER BY val".formatted(warehouse, file);
                var expectedQuery = "SELECT * FROM (VALUES (0), (1), (2), (3), (4)) AS t(val)";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testHighThroughput() throws Exception {
        String file = "high-throughput-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, file));

        // Rapid fire 5 small batches
        try (HttpArrowProducer highThroughput = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                highThroughput.enqueue(arrowBytes("select " + i + " as val"));
            }
            await().atMost(2, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                var actualQuery = "SELECT val FROM read_parquet('%s/%s/*.parquet') ORDER BY val".formatted(warehouse, file);
                var expectedQuery = "SELECT * FROM (VALUES (0), (1), (2), (3), (4)) AS t(val)";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testQueueFullBehavior() throws Exception {
        String file = "full-" + System.nanoTime();
        var limitedSender = new HttpArrowProducer(schema, baseUrl, "admin", "admin", file, Duration.ofSeconds(3), 100_000, 200_000,
                Duration.ofMillis(200), 3, 1000, java.util.List.of(), java.util.List.of(), 100, 200);

        byte[] largeData = arrowBytes("select * from generate_series(200)");

        assertThrows(IllegalStateException.class, () -> {
            limitedSender.enqueue(largeData);
        });

        limitedSender.close();
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {
        var path = "spill-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, path));
        var spillSender = new HttpArrowProducer(schema, baseUrl, "admin", "admin", path, Duration.ofSeconds(10), 100_000, 200_000,
                Duration.ofMillis(200), 3, 1000, java.util.List.of(), java.util.List.of(), 50, 100_000);

        spillSender.enqueue(arrowBytes("select * from generate_series(30)"));

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            var actualQuery = "SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series".formatted(warehouse, path);
            var expectedQuery = "SELECT * FROM generate_series(30) ORDER BY generate_series";
            TestUtils.isEqual(expectedQuery, actualQuery);
        });

        spillSender.close();
    }

    @Test
    void testProjectionsHeader() throws Exception {
        String path = "projections-test-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, path));

        try (HttpArrowProducer sender = new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of("*", "'c1' as c1", "'c2' as c2"),
                java.util.List.of(),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        var actualQuery = "SELECT generate_series, c1, c2 FROM read_parquet('%s/%s/*.parquet') WHERE c1 = 'c1' AND c2 = 'c2' ORDER BY generate_series".formatted(warehouse, path);
        var expectedQuery = "SELECT generate_series, 'c1' as c1, 'c2' as c2 FROM generate_series(5) ORDER BY generate_series";
        TestUtils.isEqual(expectedQuery, actualQuery);
    }

    @Test
    void testProjectionsAndPartitionByHeaders() throws Exception {
        String path = "both-headers-test-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, path));

        try (HttpArrowProducer sender = new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of("*", "'c1' as c1", "'c2' as  c2"),
                java.util.List.of("c1", "c2"),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            var actualQuery = "SELECT generate_series, c1, c2 FROM read_parquet('%s/%s/*/*/*.parquet') ORDER BY generate_series".formatted(warehouse, path);
            var expectedQuery = "SELECT generate_series, 'c1' as c1, 'c2' as c2 FROM generate_series(5) ORDER BY generate_series";
            TestUtils.isEqual(expectedQuery, actualQuery);
        });
    }

    @Test
    void testEmptyListsDoNotSendHeaders() throws Exception {
        String path = "empty-lists-test-" + System.nanoTime();
        Files.createDirectories(Path.of(warehouse, path));

        // Empty lists should work fine and not send headers
        try (HttpArrowProducer sender = new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                path,
                Duration.ofSeconds(3),
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            var actualQuery = "SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series".formatted(warehouse, path);
            var expectedQuery = "SELECT * FROM generate_series(5) ORDER BY generate_series";
            TestUtils.isEqual(expectedQuery, actualQuery);
        });
    }
}
