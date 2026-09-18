package io.dazzleduck.sql.otel.collector.health;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HealthServerTest {

    private static final int GRPC_PORT = 4317;

    CollectorHealth health;
    HealthServer server;
    HttpClient client;

    @BeforeEach
    void setUp() throws Exception {
        health = new CollectorHealth(() -> 2, () -> 5);
        server = new HealthServer(0, health, GRPC_PORT, java.util.List::of);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    @Test
    void healthyReturns200() throws Exception {
        health.transitionTo(CollectorHealthStatus.HEALTHY);
        HttpResponse<String> resp = get();
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("\"status\": \"HEALTHY\""));
        assertTrue(resp.body().contains("\"grpcPort\": " + GRPC_PORT));
        assertTrue(resp.body().contains("\"knownQueues\": 2"));
        assertTrue(resp.body().contains("\"batchesProcessed\": 5"));
    }

    @Test
    void maintenanceReturns503() throws Exception {
        health.transitionTo(CollectorHealthStatus.MAINTENANCE);
        HttpResponse<String> resp = get();
        assertEquals(503, resp.statusCode());
        assertTrue(resp.body().contains("\"status\": \"MAINTENANCE\""));
    }

    @Test
    void downReturns503() throws Exception {
        health.transitionTo(CollectorHealthStatus.DOWN);
        HttpResponse<String> resp = get();
        assertEquals(503, resp.statusCode());
        assertTrue(resp.body().contains("\"status\": \"DOWN\""));
    }

    @Test
    void statsServesHtmlDashboard() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort() + "/stats"))
                .GET().build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue("Content-Type").orElse("").contains("text/html"));
        assertTrue(resp.body().contains("<!DOCTYPE html>"));
        // Empty supplier (List::of) -> the placeholder row, not an error.
        assertTrue(resp.body().contains("No active ingestion queues"));
    }

    private HttpResponse<String> get() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + server.getPort() + "/health"))
                .GET().build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
