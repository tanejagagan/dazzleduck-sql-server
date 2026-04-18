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
 * Integration test for the logback-demo docker-compose example.
 * Two demo app containers emit log lines via LogForwardingAppender;
 * this test waits for logs to land in DuckLake and queries them.
 *
 * Run with: mvn test -Pdocker-compose -pl dazzleduck-sql-examples
 */
@Tag("docker-compose")
class LogbackDemoTest {

    private static final File COMPOSE_FILE =
            new File("../example/docker/logback-demo/docker-compose.yml");

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
    void logsLandInDuckLake() throws Exception {
        // Give both logback-demo-app containers time to emit and flush logs
        Thread.sleep(Duration.ofSeconds(30).toMillis());

        String body = "{\"query\":\"SELECT COUNT(*) AS cnt FROM ollylake.main.log\"}";
        HttpResponse<String> res = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/v1/query"))
                        .header("Authorization", "Bearer " + jwtToken)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().length() > 0, "Expected query result, got empty body");
    }
}
