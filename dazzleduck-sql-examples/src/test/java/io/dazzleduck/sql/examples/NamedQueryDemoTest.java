package io.dazzleduck.sql.examples;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the named-query-demo docker-compose example.
 * Verifies that named queries can be listed and executed against the
 * bundled SIEM/marketing dataset.
 *
 * Run with: mvn test -Pdocker-compose -pl dazzleduck-sql-examples
 */
@Tag("docker-compose")
class NamedQueryDemoTest {

    private static final File COMPOSE_FILE =
            new File("../example/docker/named-query-demo/docker-compose.yml");

    private static ComposeContainer compose;
    private static String baseUrl;
    private static String jwtToken;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    @BeforeAll
    static void startCompose() throws Exception {
        compose = new ComposeContainer(ComposeFiles.stripped(COMPOSE_FILE))
                .withExposedService("dazzleduck-server", 8081,
                        Wait.forHttp("/health").withStartupTimeout(Duration.ofSeconds(120)));
        compose.start();

        String host = compose.getServiceHost("dazzleduck-server", 8081);
        int port = compose.getServicePort("dazzleduck-server", 8081);
        baseUrl = "http://" + host + ":" + port;

        // verify_signature=false on server — any well-formed JWT is accepted
        jwtToken = TestTokens.unsignedToken();
    }

    @AfterAll
    static void stopCompose() {
        if (compose != null) compose.stop();
    }

    @Test
    void serverIsHealthy() throws Exception {
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder().uri(URI.create(baseUrl + "/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
    }

    @Test
    void listNamedQueriesReturnsSiemAndMarketing() throws Exception {
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/v1/named-query"))
                        .header("Authorization", "Bearer " + jwtToken)
                        .header("Accept", "application/json")
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("siem_alert_summary"),
                "Expected siem_alert_summary in named-query list, got: " + res.body());
        assertTrue(res.body().contains("marketing_campaign_performance"),
                "Expected marketing_campaign_performance in named-query list, got: " + res.body());
    }

    @Test
    void executeNamedQueryReturnsSiemAlerts() throws Exception {
        String body = "{\"name\":\"siem_alert_summary\",\"parameters\":{}}";
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/v1/named-query"))
                        .header("Authorization", "Bearer " + jwtToken)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().length() > 0, "Expected non-empty response");
    }
}
