package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.types.JavaRow;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MockIngestionServer to verify it correctly mimics the ingestion service.
 */
@Execution(ExecutionMode.CONCURRENT)
class MockIngestionServerTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private MockIngestionServer server;
    private int port;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws IOException {
        port = findAvailablePort();
        server = new MockIngestionServer(port, tempDir);
        server.start();
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testStartStop() throws Exception {
        // Create a new server (don't use the one from setUp)
        int newPort = findAvailablePort();
        Path newDir = tempDir.resolve("start_stop_test");
        Files.createDirectories(newDir);

        MockIngestionServer testServer = new MockIngestionServer(newPort, newDir);

        // Server should not be accepting connections before start
        assertThrows(Exception.class, () -> {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + newPort + "/v1/login"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
                    .timeout(Duration.ofMillis(500))
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        });

        // Start server
        testServer.start();

        // Now it should accept connections
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + newPort + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        // Stop server
        testServer.stop();

        // Server should not accept connections after stop
        assertThrows(Exception.class, () -> {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + newPort + "/v1/login"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
                    .timeout(Duration.ofMillis(500))
                    .build();
            httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        });
    }

    @Test
    void testLoginSuccess() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("accessToken"));
        assertTrue(response.body().contains("Bearer"));
    }

    @Test
    void testLoginFailure() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"wrong\",\"password\":\"wrong\"}"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(401, response.statusCode());
    }

    @Test
    void testIngestWithoutToken() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=test"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(401, response.statusCode());
        assertTrue(response.body().contains("Authorization"));
    }

    @Test
    void testIngestWithInvalidToken() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=test"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .header("Authorization", "Bearer invalid-token-12345")
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(401, response.statusCode());
        assertTrue(response.body().contains("Invalid"));
    }

    @Test
    void testIngestMissingPath() throws Exception {
        String token = login();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("path"));
    }

    @Test
    void testIngestInvalidContentType() throws Exception {
        String token = login();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=test"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(415, response.statusCode());
    }

    @Test
    void testIngestInvalidPath() throws Exception {
        String token = login();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=../evil"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("Invalid path"));
    }

    @Test
    void testIngestSuccess() throws Exception {
        String token = login();
        byte[] testData = "test arrow data".getBytes();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=test_table"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(testData))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals(testData.length, server.getTotalBytesIngested());
        assertEquals(1, server.getTotalFilesWritten());

        // Verify file was created
        Path tableDir = tempDir.resolve("test_table");
        assertTrue(Files.exists(tableDir));
        assertTrue(Files.list(tableDir).findFirst().isPresent());
    }

    @Test
    void testMultipleIngests() throws Exception {
        String token = login();

        for (int i = 0; i < 5; i++) {
            byte[] testData = ("batch " + i).getBytes();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=multi_test"))
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .header("Authorization", "Bearer " + token)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(testData))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
        }

        assertEquals(5, server.getTotalFilesWritten());

        // Verify all files were created
        Path tableDir = tempDir.resolve("multi_test");
        long fileCount = Files.list(tableDir).count();
        assertEquals(5, fileCount);
    }

    @Test
    void testWithHttpFlightProducer() throws Exception {
        // Test that HttpFlightProducer can work with MockIngestionServer
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        ));

        String testPath = "producer_test";

        try (HttpFlightProducer producer = new HttpFlightProducer(
                schema,
                server.getBaseUrl(),
                "admin",
                "admin",
                testPath,
                Duration.ofSeconds(10),
                100,   // minBatchSize
                1000,  // maxBatchSize
                Duration.ofMillis(100),
                3,     // retryCount
                100,   // retryIntervalMillis
                List.of(),
                List.of(),
                1_000_000,
                5_000_000,
                Clock.systemUTC()
        )) {
            // Add enough rows to exceed minBatchSize (100 bytes)
            for (int i = 0; i < 50; i++) {
                producer.addRow(new JavaRow(new Object[]{(long) i, "name_" + i}));
            }

            // Wait for data to be sent (producer sends when batch is ready or on close)
            Thread.sleep(1000);
        }

        // Producer close() flushes remaining data, wait a bit more
        Thread.sleep(500);

        // Verify data was ingested
        assertTrue(server.getTotalFilesWritten() > 0, "Expected at least one file to be written");
        assertTrue(server.getTotalBytesIngested() > 0, "Expected bytes to be ingested");

        Path tableDir = tempDir.resolve(testPath);
        assertTrue(Files.exists(tableDir), "Table directory should exist");
    }

    @Test
    void testResetStats() throws Exception {
        String token = login();
        byte[] testData = "test data".getBytes();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/ingest?path=stats_test"))
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofByteArray(testData))
                .build();

        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertTrue(server.getTotalBytesIngested() > 0);
        assertTrue(server.getTotalFilesWritten() > 0);

        server.resetStats();

        assertEquals(0, server.getTotalBytesIngested());
        assertEquals(0, server.getTotalFilesWritten());
    }

    /**
     * Helper method to login and get a valid token.
     */
    private String login() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(server.getBaseUrl() + "/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"admin\"}"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        JsonNode json = mapper.readTree(response.body());
        return json.get("accessToken").asText();
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
