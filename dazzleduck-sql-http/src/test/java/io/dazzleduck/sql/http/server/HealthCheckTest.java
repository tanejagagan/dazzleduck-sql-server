package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HealthCheckTest {

    private static final int PORT = 8081;
    private static final String BASE_URL = "http://localhost:" + PORT;

    private HttpClient client;
    private ObjectMapper mapper;

    @BeforeAll
    void startServer() throws Exception {
        client = HttpClient.newHttpClient();
        mapper = new ObjectMapper();

        // Start your actual server
        Main.main(new String[] {"--conf", "dazzleduck_server.http.port=" + PORT});

        // Small wait to ensure server is ready
        Thread.sleep(500);
    }

    @Test
    void testHealthEndpoint_executesSelect1_andReturnsMetrics() throws Exception {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/health"))
                .GET()
                .build();

        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode(), "Health should return 200");

        JsonNode json = mapper.readTree(response.body());

        // ---- Top-level checks
        assertEquals("UP", json.get("status").asText());
        assertTrue(json.has("uptime_seconds"));
        assertTrue(json.has("timestamp"));
        JsonNode db = json.get("database");
        assertEquals("UP", db.get("status").asText());
        assertEquals("SELECT 1", db.get("check").asText());
        JsonNode metrics = json.get("metrics");
        assertTrue(metrics.get("requests_total").asInt() >= 1);
        assertTrue(metrics.get("bytes_in").asLong() > 0);
        assertTrue(metrics.get("bytes_out").asLong() > 0);
    }

    @Test
    void testMetricsIncrementOnSubsequentCalls() throws Exception {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/health"))
                .GET()
                .build();

        HttpResponse<String> r1 = client.send(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> r2 = client.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode j1 = mapper.readTree(r1.body());
        JsonNode j2 = mapper.readTree(r2.body());

        int requests1 = j1.get("metrics").get("requests_total").asInt();
        int requests2 = j2.get("metrics").get("requests_total").asInt();

        assertTrue(requests2 > requests1);

        long bytesOut1 = j1.get("metrics").get("bytes_out").asLong();
        long bytesOut2 = j2.get("metrics").get("bytes_out").asLong();

        assertTrue(bytesOut2 > bytesOut1);
    }
}
