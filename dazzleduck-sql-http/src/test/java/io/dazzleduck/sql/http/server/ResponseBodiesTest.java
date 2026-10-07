package io.dazzleduck.sql.http.server;

import io.helidon.webserver.WebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * The response body is opened by its first byte, so a query that fails before writing anything can
 * still be answered with an error status and its own message.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ResponseBodiesTest {

    private static WebServer server;
    private static final HttpClient client = HttpClient.newHttpClient();
    private static final CompletableFuture<Throwable> writeAfterSent = new CompletableFuture<>();

    @BeforeAll
    static void startServer() {
        server = WebServer.builder()
                .port(0)
                .routing(routing -> routing
                        // What QueryService sees when a listener fails before writing: the listener's
                        // error() flushes and closes the body, then the service sends the error.
                        .get("/fails-before-writing", (req, res) -> {
                            OutputStream body = ResponseBodies.of(res);
                            body.flush();
                            body.close();
                            if (ResponseBodies.canSendError(res, body)) {
                                ControllerService.sendFlightError(res, new IllegalStateException("the real cause"));
                            }
                        })
                        // An empty result: the listener closes a body it never wrote to.
                        .get("/empty", (req, res) -> {
                            OutputStream body = ResponseBodies.of(res);
                            body.close();
                            ResponseBodies.finish(body);
                        })
                        .get("/writes", (req, res) -> {
                            OutputStream body = ResponseBodies.of(res);
                            body.write("rows".getBytes(StandardCharsets.UTF_8));
                            body.close();
                            ResponseBodies.finish(body); // no-op: already completed by close
                        })
                        // The service answered (e.g. a timeout) while the query kept writing.
                        .get("/already-sent", (req, res) -> {
                            OutputStream body = ResponseBodies.of(res);
                            res.status(504).send("timeout");
                            try {
                                body.write(1);
                                writeAfterSent.complete(null);
                            } catch (Throwable t) {
                                writeAfterSent.complete(t);
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
    void writingAfterTheResponseWasSentIsAGoneClient() throws Exception {
        var response = get("/already-sent");
        assertEquals(504, response.statusCode());
        // An IOException is what the listeners treat as the client going away, not a server error.
        assertInstanceOf(IOException.class, writeAfterSent.get(10, TimeUnit.SECONDS));
    }

    @Test
    void theBodyIsOpenedByTheFirstByteOnly() throws Exception {
        var opened = new AtomicInteger();
        var target = new ByteArrayOutputStream();
        var body = new ResponseBodies.ClientGoneAsIOException(() -> {
            opened.incrementAndGet();
            return target;
        });
        body.flush();
        body.close();
        assertEquals(0, opened.get(), "flush and close with nothing written do not open it");
        ResponseBodies.finish(body);
        assertEquals(1, opened.get(), "finish opens (and closes) an unwritten body");

        var written = new ResponseBodies.ClientGoneAsIOException(() -> {
            opened.incrementAndGet();
            return target;
        });
        written.write('a');
        written.close();
        ResponseBodies.finish(written);
        assertEquals(2, opened.get(), "a write opens it once; finish leaves it alone");
        assertEquals("a", target.toString(StandardCharsets.UTF_8));
    }
}
