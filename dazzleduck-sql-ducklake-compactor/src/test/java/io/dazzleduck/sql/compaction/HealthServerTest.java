package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthServerTest {

    private HealthServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void tearDown() {
        if (server != null) server.close();
    }

    // Tier name "tier" and database "db" match TestRuns.run(...), so the /health per-tier lookup finds
    // the recorded run and attaches its telemetry.
    private static CompactionStats statsWithTier() {
        var ds = new CompactionStats.DatabaseStats(
                Map.of("tier", 1L), 0, 5, Instant.now(), Map.of(), Map.of("tier", 3L), 3);
        return new CompactionStats(Instant.now(), Map.of("db", ds));
    }

    private String get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                URI.create("http://localhost:" + serverPort() + path)).GET().build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        return resp.body();
    }

    private int serverPort;
    private int serverPort() { return serverPort; }

    @Test
    void healthJsonCarriesPerTierTelemetryWhenRunsExist() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 100L, 20L, 1000L, 200L,
                CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String health = get("/health");
        assertTrue(health.contains("\"status\": \"UP\""));
        assertTrue(health.contains("\"telemetry\""), "per-tier telemetry section present");
        assertTrue(health.contains("\"drainFilesPerSec\""), "derived aggregates present");
        assertTrue(health.contains("\"outcome\": \"SUCCESS\""));
    }

    @Test
    void uiServesHtmlDashboard() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 100L, 20L, 1000L, 200L,
                CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String ui = get("/ui");
        assertTrue(ui.startsWith("<!DOCTYPE html>"));
        assertTrue(ui.contains("DuckLake Compaction Telemetry"));
    }

    @Test
    void telemetryIsNullPerTierBeforeAnyRun() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50); // empty
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String health = get("/health");
        assertTrue(health.contains("\"telemetry\": null"), "no run yet -> telemetry null, not an error");
    }

    /** The HttpServer picked an ephemeral port (0); read it back via reflection-free accessor. */
    private static int portOf(HealthServer server) {
        return server.boundPort();
    }
}
