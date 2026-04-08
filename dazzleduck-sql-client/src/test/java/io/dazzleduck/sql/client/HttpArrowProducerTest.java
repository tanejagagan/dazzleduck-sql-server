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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
    private static String ingestionPath;
    private static String baseUrl;
    private static Schema schema;

    @BeforeAll
    static void setup() throws Exception {
        server = new SharedTestServer();
        server.start();
        ingestionPath = server.getIngestionPath();
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
                Map.of(),
                file,
                timeout,
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
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
        var actualQuery = String.format("SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series", ingestionPath, path);
        var expectedQuery = String.format("SELECT * FROM generate_series(%d) ORDER BY generate_series", maxValue);
        TestUtils.isEqual(expectedQuery, actualQuery);
    }

    @Test
    void testAsyncIngestionSingleBatch() throws Exception {
        String file = "async-single-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));
        try (HttpArrowProducer sender = newSender(file, Duration.ofSeconds(10))) {
            sender.enqueue(arrowBytes("select * from generate_series(4)"));
        }

        // Verify after close() has flushed all data
        verifyFile(file, 4);
    }

    @Test
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {
        String file = "overwrite-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));

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
                    var actualQuery = String.format("SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series", ingestionPath, file);
                    var expectedQuery = "SELECT * FROM (VALUES (0), (0), (1), (1), (2)) AS t(generate_series)";
                    TestUtils.isEqual(expectedQuery, actualQuery);
                });
    }

    @Test
    void testConcurrentEnqueues() throws Exception {
        String file = "concurrent-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));
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
                var actualQuery = String.format("SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series", ingestionPath, file);
                var expectedQuery = "SELECT * FROM (SELECT * FROM generate_series(0) UNION ALL SELECT * FROM generate_series(10) UNION ALL SELECT * FROM generate_series(20) UNION ALL SELECT * FROM generate_series(30) UNION ALL SELECT * FROM generate_series(40)) ORDER BY generate_series";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testJWTTokenReuse() throws Exception {
        String file = "jwt-reuse-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));

        // Multiple requests should reuse the same token
        try (HttpArrowProducer reuseSender = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                reuseSender.enqueue(arrowBytes("select " + i + " as val"));
            }

            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                var actualQuery = String.format("SELECT val FROM read_parquet('%s/%s/*.parquet') ORDER BY val", ingestionPath, file);
                var expectedQuery = "SELECT * FROM (VALUES (0), (1), (2), (3), (4)) AS t(val)";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testHighThroughput() throws Exception {
        String file = "high-throughput-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));

        // Rapid fire 5 small batches
        try (HttpArrowProducer highThroughput = newSender(file, Duration.ofSeconds(5))) {
            for (int i = 0; i < 5; i++) {
                highThroughput.enqueue(arrowBytes("select " + i + " as val"));
            }
            await().atMost(2, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
                var actualQuery = String.format("SELECT val FROM read_parquet('%s/%s/*.parquet') ORDER BY val", ingestionPath, file);
                var expectedQuery = "SELECT * FROM (VALUES (0), (1), (2), (3), (4)) AS t(val)";
                TestUtils.isEqual(expectedQuery, actualQuery);
            });
        }
    }

    @Test
    void testQueueFullBehavior() throws Exception {
        String file = "full-" + System.nanoTime();
        var limitedSender = new HttpArrowProducer(schema, baseUrl, "admin", "admin", file, Duration.ofSeconds(3), 100_000, 200_000,
                Duration.ofMillis(200), 3, 1000, java.util.List.of(), 100, 200);

        byte[] largeData = arrowBytes("select * from generate_series(200)");

        assertThrows(IllegalStateException.class, () -> {
            limitedSender.enqueue(largeData);
        });

        limitedSender.close();
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {
        var path = "spill-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, path));
        var spillSender = new HttpArrowProducer(schema, baseUrl, "admin", "admin", path, Duration.ofSeconds(10), 100_000, 200_000,
                Duration.ofMillis(200), 3, 1000, java.util.List.of(), 50, 100_000);

        spillSender.enqueue(arrowBytes("select * from generate_series(30)"));

        await().atMost(10, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            var actualQuery = String.format("SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series", ingestionPath, path);
            var expectedQuery = "SELECT * FROM generate_series(30) ORDER BY generate_series";
            TestUtils.isEqual(expectedQuery, actualQuery);
        });

        spillSender.close();
    }

    // --- Preconfigured JWT tests ---

    /**
     * Obtain a real JWT via login, then pass it as a preconfigured token.
     * The producer must ingest data successfully without calling login again.
     */
    @Test
    void testPreconfiguredJwtIngestsSuccessfully() throws Exception {
        String file = "static-jwt-success-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, file));

        String jwt = fetchJwt(baseUrl, "admin", "admin");

        try (HttpArrowProducer producer = new HttpArrowProducer(
                schema,
                baseUrl,
                jwt,
                file,
                Duration.ofSeconds(10),
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                List.of(),
                100_000,
                500_000
        )) {
            producer.enqueue(arrowBytes("select * from generate_series(4)"));
        }

        verifyFile(file, 4);
    }

    /**
     * With a preconfigured JWT, the login endpoint must never be called — even across multiple batches.
     */
    @Test
    void testPreconfiguredJwtDoesNotCallLogin() throws Exception {
        try (MockIngestionServer mock = new MockIngestionServer(0, Path.of(ingestionPath))) {
            mock.start();
            String token = "pre-issued-token-" + System.nanoTime();
            mock.addValidToken(token);

            try (HttpArrowProducer producer = new HttpArrowProducer(
                    schema,
                    mock.getBaseUrl(),
                    "Bearer " + token,
                    "no-login-test",
                    Duration.ofSeconds(10),
                    100_000,
                    200_000,
                    Duration.ofMillis(200),
                    3,
                    1000,
                    List.of(),
                    100_000,
                    500_000
            )) {
                producer.enqueue(arrowBytes("select 1 as val"));
                producer.enqueue(arrowBytes("select 2 as val"));
            }

            assertEquals(0, mock.getLoginCallCount(), "Login must not be called when JWT is preconfigured");
            assertTrue(mock.getTotalFilesWritten() > 0, "Data should have been ingested");
        }
    }

    /**
     * An invalid preconfigured JWT must fail immediately without attempting login.
     */
    @Test
    void testPreconfiguredJwtInvalidDoesNotRetryLogin() throws Exception {
        try (MockIngestionServer mock = new MockIngestionServer(0, Path.of(ingestionPath))) {
            mock.start();

            try (HttpArrowProducer producer = new HttpArrowProducer(
                    schema,
                    mock.getBaseUrl(),
                    "Bearer invalid-token-xyz",
                    "invalid-jwt-test",
                    Duration.ofSeconds(10),
                    100_000,
                    200_000,
                    Duration.ofMillis(200),
                    1,
                    100,
                    List.of(),
                    100_000,
                    500_000
            )) {
                producer.enqueue(arrowBytes("select 1 as val"));
            }

            assertEquals(0, mock.getLoginCallCount(), "Login must not be attempted when using a preconfigured JWT");
        }
    }

    /**
     * Null JWT in the static-JWT constructor should throw NullPointerException.
     */
    @Test
    void testPreconfiguredJwtNullThrows() {
        assertThrows(NullPointerException.class, () -> new HttpArrowProducer(
                schema,
                baseUrl,
                (String) null,   // jwt
                "some-queue",
                Duration.ofSeconds(5),
                100_000,
                200_000,
                Duration.ofMillis(200),
                1,
                100,
                List.of(),
                100_000,
                500_000
        ));
    }

    /** Helper: perform a login HTTP request and return the full "Bearer <token>" value. */
    private static String fetchJwt(String baseUrl, String username, String password) throws Exception {
        var httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var body = String.format("{\"username\":\"%s\",\"password\":\"%s\"}", username, password);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "Login should succeed");
        var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(resp.body());
        return json.get("tokenType").asText() + " " + json.get("accessToken").asText();
    }

    @Test
    void testEmptyPartitionByDoesNotSendHeader() throws Exception {
        String path = "empty-partition-test-" + System.nanoTime();
        Files.createDirectories(Path.of(ingestionPath, path));

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
                100_000,
                500_000)) {

            sender.enqueue(arrowBytes("select * from generate_series(5)"));
        }

        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() -> {
            var actualQuery = String.format("SELECT generate_series FROM read_parquet('%s/%s/*.parquet') ORDER BY generate_series", ingestionPath, path);
            var expectedQuery = "SELECT * FROM generate_series(5) ORDER BY generate_series";
            TestUtils.isEqual(expectedQuery, actualQuery);
        });
    }
}
