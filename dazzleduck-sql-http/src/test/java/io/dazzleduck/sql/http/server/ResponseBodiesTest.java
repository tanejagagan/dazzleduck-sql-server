package io.dazzleduck.sql.http.server;

import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The response body is opened on first use, so a query that fails before writing anything can still
 * be answered with an error status and its own message.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ResponseBodiesTest {

    private static WebServer server;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() {
        server = WebServer.builder()
                .port(0)
                .routing(routing -> routing
                        // What QueryService does when a listener fails while setting up its writer.
                        .get("/fails-before-writing", (req, res) -> {
                            OutputStream body = ResponseBodies.of(res);
                            ControllerService.sendFlightError(res, new IllegalStateException("the real cause"));
                        })
                        .get("/empty", (req, res) -> ResponseBodies.of(res).close())
                        .get("/writes", (req, res) -> {
                            try (OutputStream body = ResponseBodies.of(res)) {
                                body.write("rows".getBytes(StandardCharsets.UTF_8));
                            }
                        }))
                .build()
                .start();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop();
        }
    }

    private static HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void failureBeforeTheFirstByteIsReportedWithItsMessage() throws Exception {
        var response = get("/fails-before-writing");
        assertEquals(500, response.statusCode());
        assertEquals("the real cause", response.body());
    }

    @Test
    void closingWithoutWritingSendsAnEmptyBody() throws Exception {
        var response = get("/empty");
        assertEquals(200, response.statusCode());
        assertEquals("", response.body());
    }

    @Test
    void writesStillReachTheClient() throws Exception {
        var response = get("/writes");
        assertEquals(200, response.statusCode());
        assertEquals("rows", response.body());
    }

    @Test
    void theBodyIsOpenedOnFirstUseOnly() throws Exception {
        var opened = new AtomicInteger();
        var target = new ByteArrayOutputStream();
        var body = new ResponseBodies.ClientGoneAsIOException(() -> {
            opened.incrementAndGet();
            return target;
        });
        assertEquals(0, opened.get(), "not opened when created");
        body.write('a');
        body.flush();
        body.close();
        assertEquals(1, opened.get(), "opened once");
        assertEquals("a", target.toString(StandardCharsets.UTF_8));
    }
}
