package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.flight.server.DirectOutputStreamListener;
import io.dazzleduck.sql.flight.server.HttpResponseListener;
import io.dazzleduck.sql.flight.server.JsonOutputStreamListener;
import io.dazzleduck.sql.flight.server.TsvOutputStreamListener;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http2.Http2Config;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A client that goes away mid-response, through Helidon's real HTTP/1.1 and HTTP/2 (h2c) servers.
 *
 * <p>The listeners must recognise the exceptions Helidon actually throws on such a write as the
 * client going away: the response ends, and the failure is not counted as a server error. Tests
 * that write to their own {@code OutputStream} can't show this, since each Helidon protocol reports a
 * gone client its own way.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HttpListenerDisconnectTest {

    private static final int ROWS_PER_BATCH = 10_000;
    private static final int MAX_BATCHES = 100_000;

    /** What the server saw when its client went away. */
    record Outcome(int batchesWritten, Throwable escaped, Throwable writeFailure, boolean responseOver) { }

    private static WebServer server;
    private static volatile CompletableFuture<Outcome> outcome;

    @BeforeAll
    static void startServer() {
        server = WebServer.builder()
                .port(0)
                // A reset HTTP/2 stream is only noticed when a write's wait for flow control times out
                // (15 s by default): shortened so the test is quick.
                .addProtocol(Http2Config.builder().flowControlTimeout(Duration.ofSeconds(1)).build())
                .routing(routing -> routing.get("/stream/{format}", (req, res) -> {
                    String format = req.path().pathParameters().get("format");
                    outcome.complete(writeUntilTheClientGoesAway(format, res::outputStream));
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

    private static FlightProducer.ServerStreamListener listener(String format, Supplier<OutputStream> out,
                                                                CompletableFuture<Void> done) {
        return switch (format) {
            case "arrow" -> new DirectOutputStreamListener(out, done);
            case "json" -> new JsonOutputStreamListener(out, done);
            case "tsv" -> new TsvOutputStreamListener(out, done);
            default -> throw new IllegalArgumentException(format);
        };
    }

    /** Like the producer's stream loop: write batches until the response is over. */
    private static Outcome writeUntilTheClientGoesAway(String format, Supplier<OutputStream> out) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = batch(allocator)) {
            var done = new CompletableFuture<Void>();
            var listener = listener(format, out, done);
            int written = 0;
            try {
                listener.start(root);
                while (!listener.isCancelled() && written < MAX_BATCHES) {
                    listener.putNext();
                    written++;
                }
            } catch (Throwable escaped) {
                return new Outcome(written, escaped, ((HttpResponseListener) listener).writeFailure(), listener.isCancelled());
            }
            return new Outcome(written, null, ((HttpResponseListener) listener).writeFailure(), listener.isCancelled());
        }
    }

    private static VectorSchemaRoot batch(BufferAllocator allocator) {
        var ids = new BigIntVector("id", allocator);
        var names = new VarCharVector("name", allocator);
        ids.allocateNew(ROWS_PER_BATCH);
        names.allocateNew(ROWS_PER_BATCH);
        for (int i = 0; i < ROWS_PER_BATCH; i++) {
            ids.set(i, i);
            names.setSafe(i, ("row-" + i + "-abcdefghijklmnopqrstuvwxyz").getBytes(StandardCharsets.UTF_8));
        }
        var root = new VectorSchemaRoot(List.of(ids.getField(), names.getField()), List.of(ids, names));
        root.setRowCount(ROWS_PER_BATCH);
        return root;
    }

    @ParameterizedTest(name = "{0} over {1}, {2}")
    @CsvSource({
            "arrow, HTTP_1_1, closes the connection", "json, HTTP_1_1, closes the connection",
            "tsv, HTTP_1_1, closes the connection",
            "arrow, HTTP_2, closes the connection", "json, HTTP_2, closes the connection",
            "tsv, HTTP_2, closes the connection",
            "arrow, HTTP_2, resets the stream", "json, HTTP_2, resets the stream",
            "tsv, HTTP_2, resets the stream",
    })
    void aClientThatGoesAwayEndsTheResponseWithoutAServerError(String format, HttpClient.Version version,
                                                                String how) throws Exception {
        boolean connectionStaysOpen = how.equals("resets the stream");
        outcome = new CompletableFuture<>();
        Outcome seen = null;
        try (var client = HttpClient.newBuilder().version(version).build()) {
            var request = HttpRequest.newBuilder(URI.create("http://localhost:%d/stream/%s".formatted(server.port(), format)))
                    .GET().build();
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            assertEquals(version, response.version());
            try (InputStream body = response.body()) {
                assertTrue(body.readNBytes(1 << 20).length > 0); // the response is streaming
            } // HTTP/1.1 closes the connection here; HTTP/2 resets just this stream
            if (connectionStaysOpen) {
                seen = outcome.get(30, TimeUnit.SECONDS);
            }
        } // closes the HTTP/2 connection
        if (!connectionStaysOpen) {
            seen = outcome.get(30, TimeUnit.SECONDS);
        }

        Outcome result = seen;
        assertNull(result.escaped(), () -> "a gone client must not escape the listener: " + result);
        assertTrue(result.responseOver(), () -> "the response must be over: " + result);
        assertTrue(result.batchesWritten() < MAX_BATCHES, () -> "the stream must stop: " + result);
        assertNotNull(result.writeFailure(), () -> "the failed write is recorded: " + result);
        assertTrue(HttpResponseListener.isClientGone(result.writeFailure()),
                () -> "a gone client is not a server error: " + result.writeFailure());
    }
}
