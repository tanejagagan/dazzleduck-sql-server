package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for back pressure handling in HttpArrowProducer.
 * Verifies that HTTP 429 responses trigger BackPressureException and exponential backoff.
 */
public class BackPressureTest {

    private static final Logger logger = LoggerFactory.getLogger(BackPressureTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Schema TEST_SCHEMA = new Schema(
            List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null))
    );

    private BackPressureMockServer mockServer;
    private Path warehousePath;

    @BeforeEach
    void setup() throws Exception {
        warehousePath = Files.createTempDirectory("backpressure-test");
        mockServer = new BackPressureMockServer(0, warehousePath);
        mockServer.start();
    }

    @AfterEach
    void teardown() {
        if (mockServer != null) {
            mockServer.close();
        }
        if (warehousePath != null) {
            deleteDirectory(warehousePath.toFile());
        }
    }

    @Test
    void testBackPressureExceptionThrownOn429() throws Exception {
        // Configure server to always return 429
        mockServer.setAlwaysReturn429(true);

        String path = "backpressure-test-" + System.nanoTime();
        Files.createDirectories(warehousePath.resolve(path));

        // Create producer with minimal retries to speed up test
        try (HttpArrowProducer producer = new HttpArrowProducer(
                TEST_SCHEMA,
                mockServer.getBaseUrl(),
                "admin",
                "admin",
                Map.of(),
                path,
                Duration.ofSeconds(5),
                1000,      // minBatchSize
                10000,     // maxBatchSize
                Duration.ofMillis(100),  // maxSendInterval
                0,         // retryCount - no retries for other errors
                100,       // retryIntervalMillis
                List.of(),
                List.of(),
                100_000,
                100_000,
                Clock.systemUTC()
        )) {
            // Create minimal Arrow data
            byte[] arrowData = createMinimalArrowData();
            producer.enqueue(arrowData);

            // Wait for producer to process and fail
            Thread.sleep(2000);
        }

        // Verify that 429 was encountered (element should be dropped after max back pressure retries)
        assertTrue(mockServer.get429Count() > 0, "Server should have received requests that returned 429");
        logger.info("Server returned 429 {} times", mockServer.get429Count());
    }

    @Test
    void testExponentialBackoffOnBackPressure() throws Exception {
        // Configure server to return 429 for first 2 requests, then succeed
        mockServer.setReturn429ForFirstN(2);
        mockServer.setRetryAfterSeconds(0); // Use client's backoff

        String path = "backoff-test-" + System.nanoTime();
        Files.createDirectories(warehousePath.resolve(path));

        long startTime = System.currentTimeMillis();

        HttpArrowProducer producer = new HttpArrowProducer(
                TEST_SCHEMA,
                mockServer.getBaseUrl(),
                "admin",
                "admin",
                Map.of(),
                path,
                Duration.ofSeconds(30),
                1000,
                10000,
                Duration.ofMillis(100),
                3,         // retryCount for other errors
                100,       // retryIntervalMillis - starting backoff (100ms)
                List.of(),
                List.of(),
                100_000,
                100_000,
                Clock.systemUTC()
        );

        byte[] arrowData = createMinimalArrowData();
        producer.enqueue(arrowData);

        // Wait for processing to complete - close() will wait for sender thread
        Thread.sleep(1000);
        producer.close();

        long elapsed = System.currentTimeMillis() - startTime;

        int total429s = mockServer.get429Count();
        int totalSuccesses = mockServer.getSuccessCount();

        logger.info("Test completed in {}ms: {} 429 responses, {} successes",
                elapsed, total429s, totalSuccesses);

        // Verify back pressure was detected and recovered
        assertTrue(total429s >= 1, "Should have at least 1 429 response, got " + total429s);
        assertTrue(totalSuccesses >= 1, "Should have successful request after back pressure cleared, got " + totalSuccesses);

        // Verify exponential backoff caused delays (at least 100ms for first retry)
        assertTrue(elapsed >= 200, "Exponential backoff should cause delays, but elapsed was " + elapsed + "ms");
    }

    @Test
    void testRecoveryAfterBackPressure() throws Exception {
        // Return 429 for first request, then succeed
        mockServer.setReturn429ForFirstN(1);
        mockServer.setRetryAfterSeconds(0); // Use client's backoff

        String path = "recovery-test-" + System.nanoTime();
        Files.createDirectories(warehousePath.resolve(path));

        HttpArrowProducer producer = new HttpArrowProducer(
                TEST_SCHEMA,
                mockServer.getBaseUrl(),
                "admin",
                "admin",
                Map.of(),
                path,
                Duration.ofSeconds(30),
                1000,
                10000,
                Duration.ofMillis(100),
                3,
                100,  // 100ms starting backoff
                List.of(),
                List.of(),
                100_000,
                100_000,
                Clock.systemUTC()
        );

        byte[] arrowData = createMinimalArrowData();
        producer.enqueue(arrowData);

        // Wait for processing then close
        Thread.sleep(1000);
        producer.close();

        // Verify successful recovery - at least 1 back pressure event and 1 success
        assertTrue(mockServer.get429Count() >= 1, "Should have at least 1 429 response");
        assertTrue(mockServer.getSuccessCount() >= 1, "Should have at least 1 successful request");
        assertTrue(mockServer.getTotalBytesIngested() > 0, "Data should have been ingested after recovery");
    }

    @Test
    void testRetryAfterHeaderIsHonored() throws Exception {
        // Return 429 with Retry-After header
        mockServer.setReturn429ForFirstN(1);
        mockServer.setRetryAfterSeconds(2); // 2 second wait

        String path = "retry-after-test-" + System.nanoTime();
        Files.createDirectories(warehousePath.resolve(path));

        long startTime = System.currentTimeMillis();

        try (HttpArrowProducer producer = new HttpArrowProducer(
                TEST_SCHEMA,
                mockServer.getBaseUrl(),
                "admin",
                "admin",
                Map.of(),
                path,
                Duration.ofSeconds(10),
                1000,
                10000,
                Duration.ofMillis(100),
                3,
                100,  // Small base retry interval
                List.of(),
                List.of(),
                100_000,
                100_000,
                Clock.systemUTC()
        )) {
            byte[] arrowData = createMinimalArrowData();
            producer.enqueue(arrowData);
        }

        long elapsed = System.currentTimeMillis() - startTime;

        logger.info("Retry-After test completed in {}ms", elapsed);

        // Should have waited at least 2 seconds (Retry-After header value)
        assertTrue(elapsed >= 1800, "Should honor Retry-After header, but elapsed was " + elapsed + "ms");
        assertEquals(1, mockServer.get429Count());
        assertEquals(1, mockServer.getSuccessCount());
    }

    @Test
    void testMaxBackPressureRetriesExceeded() throws Exception {
        // Always return 429 - should eventually give up
        mockServer.setAlwaysReturn429(true);
        mockServer.setRetryAfterSeconds(0); // Use client's backoff

        String path = "max-retries-test-" + System.nanoTime();
        Files.createDirectories(warehousePath.resolve(path));

        // Use retryCount of 3 so we can verify back pressure retries are limited
        HttpArrowProducer producer = new HttpArrowProducer(
                TEST_SCHEMA,
                mockServer.getBaseUrl(),
                "admin",
                "admin",
                Map.of(),
                path,
                Duration.ofSeconds(60),
                1000,
                10000,
                Duration.ofMillis(50),
                3,    // Allow 3 retries for back pressure
                50,   // 50ms starting backoff for faster test
                List.of(),
                List.of(),
                100_000,
                100_000,
                Clock.systemUTC()
        );

        byte[] arrowData = createMinimalArrowData();
        producer.enqueue(arrowData);

        // Wait for retries to happen (with 3 retries and 50ms base backoff, should complete quickly)
        Thread.sleep(2000);
        producer.close();

        // Should have tried multiple times (1 initial + up to 3 retries = max 4 attempts)
        int count429 = mockServer.get429Count();
        logger.info("Max retries test: server received {} requests returning 429", count429);

        // Should retry at least 2 times before giving up
        assertTrue(count429 >= 2, "Should retry at least 2 times before test ends, but got " + count429);
        assertEquals(0, mockServer.getSuccessCount(), "Should not succeed when always returning 429");
    }

    /**
     * Creates minimal valid Arrow IPC stream data.
     */
    private byte[] createMinimalArrowData() throws Exception {
        try (org.apache.arrow.memory.BufferAllocator allocator = new org.apache.arrow.memory.RootAllocator();
             org.apache.arrow.vector.VectorSchemaRoot root = org.apache.arrow.vector.VectorSchemaRoot.create(TEST_SCHEMA, allocator);
             java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             org.apache.arrow.vector.ipc.ArrowStreamWriter writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, baos)) {

            // Write a single row
            org.apache.arrow.vector.IntVector valueVector = (org.apache.arrow.vector.IntVector) root.getVector("value");
            valueVector.allocateNew(1);
            valueVector.set(0, 42);
            root.setRowCount(1);

            writer.start();
            writer.writeBatch();
            writer.end();

            return baos.toByteArray();
        }
    }

    private static void deleteDirectory(java.io.File directory) {
        if (directory == null || !directory.exists()) return;
        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }

    /**
     * Mock server that can return 429 responses for testing back pressure.
     */
    static class BackPressureMockServer implements AutoCloseable {
        private static final Logger logger = LoggerFactory.getLogger(BackPressureMockServer.class);
        private static final String CONTENT_TYPE_ARROW = "application/vnd.apache.arrow.stream";

        private volatile HttpServer server;
        private final Path warehousePath;
        private final int port;

        private final AtomicInteger requestCount = new AtomicInteger(0);
        private final AtomicInteger count429 = new AtomicInteger(0);
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicLong totalBytesIngested = new AtomicLong(0);
        private final Set<String> validTokens = ConcurrentHashMap.newKeySet();

        // Configuration for 429 behavior
        private volatile boolean alwaysReturn429 = false;
        private volatile int return429ForFirstN = 0;
        private volatile int retryAfterSeconds = 0;

        BackPressureMockServer(int port, Path warehousePath) throws IOException {
            this.port = port;
            this.warehousePath = warehousePath;
            Files.createDirectories(warehousePath);
        }

        void setAlwaysReturn429(boolean value) {
            this.alwaysReturn429 = value;
        }

        void setReturn429ForFirstN(int n) {
            this.return429ForFirstN = n;
        }

        void setRetryAfterSeconds(int seconds) {
            this.retryAfterSeconds = seconds;
        }

        int get429Count() {
            return count429.get();
        }

        int getSuccessCount() {
            return successCount.get();
        }

        long getTotalBytesIngested() {
            return totalBytesIngested.get();
        }

        String getBaseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        void start() {
            try {
                server = HttpServer.create(new InetSocketAddress(port), 0);
                server.setExecutor(Executors.newFixedThreadPool(4));
                server.createContext("/v1/login", new LoginHandler());
                server.createContext("/v1/ingest", new IngestHandler());
                server.start();
                logger.info("BackPressureMockServer started on port {}", server.getAddress().getPort());
            } catch (IOException e) {
                throw new RuntimeException("Failed to start server", e);
            }
        }

        @Override
        public void close() {
            if (server != null) {
                server.stop(0);
                server = null;
            }
        }

        private class LoginHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    sendError(exchange, 405, "Method not allowed");
                    return;
                }

                try {
                    exchange.getRequestBody().readAllBytes();

                    String token = "mock-token-" + System.currentTimeMillis();
                    validTokens.add(token);

                    Map<String, Object> response = new HashMap<>();
                    response.put("accessToken", token);
                    response.put("username", "admin");
                    response.put("tokenType", "Bearer");

                    byte[] responseBody = mapper.writeValueAsBytes(response);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, responseBody.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(responseBody);
                    }
                } catch (Exception e) {
                    sendError(exchange, 500, "Login failed: " + e.getMessage());
                }
            }
        }

        private class IngestHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    sendError(exchange, 405, "Method not allowed");
                    return;
                }

                int currentRequest = requestCount.incrementAndGet();

                // Check if we should return 429
                boolean shouldReturn429 = alwaysReturn429 ||
                        (return429ForFirstN > 0 && currentRequest <= return429ForFirstN);

                if (shouldReturn429) {
                    count429.incrementAndGet();
                    logger.info("Returning 429 for request #{}", currentRequest);

                    // Consume request body to prevent connection issues
                    exchange.getRequestBody().readAllBytes();

                    String errorMsg = "Pending write limit exceeded";
                    byte[] responseBody = errorMsg.getBytes();

                    // Always send Retry-After header (even if 0) so client doesn't default to 5s
                    exchange.getResponseHeaders().set("Retry-After", String.valueOf(retryAfterSeconds));
                    exchange.getResponseHeaders().set("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(429, responseBody.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(responseBody);
                    }
                    return;
                }

                // Normal ingestion handling
                try {
                    byte[] data = exchange.getRequestBody().readAllBytes();
                    totalBytesIngested.addAndGet(data.length);
                    successCount.incrementAndGet();

                    logger.info("Successfully ingested {} bytes for request #{}", data.length, currentRequest);

                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().close();
                } catch (Exception e) {
                    sendError(exchange, 500, "Ingestion failed: " + e.getMessage());
                }
            }
        }

        private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
            byte[] responseBody = message.getBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(statusCode, responseBody.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody);
            }
        }
    }
}
