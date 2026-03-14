package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.helidon.http.HeaderNames;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tests that verify the HTTP server operates correctly over HTTP/2 (h2c - cleartext).
 *
 * <p>Helidon 4 enables HTTP/2 by default on all listeners. When TLS is disabled the
 * protocol is h2c (HTTP/2 cleartext via Prior Knowledge).
 */
public class HttpServerHttp2Test extends HttpServerTestBase {

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initPort();
        startServer(); // TLS disabled by default → h2c mode

        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();

        installArrowExtension();

        ConnectionPool.execute("CREATE TABLE IF NOT EXISTS h2_test (id INTEGER, name VARCHAR, score DOUBLE)");
        ConnectionPool.execute("INSERT INTO h2_test VALUES (1,'Alice',9.5),(2,'Bob',8.0),(3,'Carol',7.5)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS h2_test");
        cleanupWarehouse();
    }

    /**
     * The most fundamental HTTP/2 test: a simple GET query must be served over HTTP/2.
     * The client sends the HTTP/2 connection preface (Prior Knowledge) and the server
     * must respond in HTTP/2 binary framing — confirmed by {@code response.version()}.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testGetQueryProtocolIsHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "GET /v1/query: server must respond in HTTP/2 binary framing (h2c Prior Knowledge)");
        assertEquals(200, response.statusCode());
    }

    /**
     * GET query: verify the Arrow IPC response is both correct and served over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testGetQueryDataCorrectnessOverHttp2() throws Exception {
        var query = "SELECT * FROM h2_test ORDER BY id";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    /**
     * HTTP/2 multiplexing: send many requests concurrently over the same connection.
     * All requests must succeed and all must be served over HTTP/2.
     *
     * <p>This is the key correctness test for HTTP/2 multiplexing: a single
     * {@link HttpClient} with HTTP/2 reuses one TCP connection and interleaves all
     * concurrent requests as independent HTTP/2 streams.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMultiplexedConcurrentQueries() throws Exception {
        final int concurrency = 50;
        var executor = Executors.newFixedThreadPool(concurrency);
        var token = getJWTToken();

        List<CompletableFuture<HttpClient.Version>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            final int n = i;
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    var request = HttpRequest.newBuilder(uriForQuery("SELECT " + n))
                            .GET()
                            .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + token)
                            .build();
                    return client.send(request, HttpResponse.BodyHandlers.ofString()).version();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor));
        }

        var versions = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).toList())
                .get(30, TimeUnit.SECONDS);

        executor.shutdown();
        assertTrue(versions.stream().allMatch(v -> v == HttpClient.Version.HTTP_2),
                "Every concurrent request must be served over HTTP/2");
        assertEquals(concurrency, versions.size());
    }

    /**
     * An HTTP/1.1 client must still get correct responses from the HTTP/2-capable server.
     *
     * <p>The server must serve both HTTP/1.1 and HTTP/2 clients from the same listener.
     * The response version must be HTTP_1_1 (the server negotiates down to what the client
     * supports) and the data must be identical to what an HTTP/2 client receives.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHttp11ClientWorksAgainstHttp2Server() throws Exception {
        var http11Client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        var token = getJWTToken();
        var query = "SELECT * FROM h2_test ORDER BY id";
        var request = HttpRequest.newBuilder(uriForQuery(query))
                .GET()
                .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + token)
                .build();
        var response = http11Client.send(request, HttpResponse.BodyHandlers.ofInputStream());

        assertEquals(HttpClient.Version.HTTP_1_1, response.version(),
                "HTTP/1.1 client must get an HTTP/1.1 response (server negotiates down)");
        assertEquals(200, response.statusCode());

        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    private static URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }
}
