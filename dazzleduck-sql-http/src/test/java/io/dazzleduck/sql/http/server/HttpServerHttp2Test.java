package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.common.auth.LoginRequest;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify the HTTP server operates correctly over HTTP/2 (h2c - cleartext).
 *
 * <p>Helidon 4 enables HTTP/2 by default on all listeners. When TLS is disabled the
 * protocol is h2c (HTTP/2 cleartext via Prior Knowledge). When TLS is enabled it is
 * h2 (HTTP/2 over TLS via ALPN).
 *
 * <p>These tests use {@link HttpClient.Version#HTTP_2} to force the Java HTTP client
 * to use HTTP/2 Prior Knowledge (h2c — the client sends the HTTP/2 connection preface
 * immediately without any negotiation) and then assert {@code response.version() == HTTP_2}
 * on every response to confirm the server responded in HTTP/2 binary framing.
 *
 * <p>Concepts covered:
 * <ul>
 *   <li>Protocol version negotiation (h2c Prior Knowledge)</li>
 *   <li>GET and POST queries over HTTP/2</li>
 *   <li>JWT authentication flow over HTTP/2</li>
 *   <li>Unauthenticated and error responses preserve HTTP/2</li>
 *   <li>HTTP/2 multiplexing — many concurrent streams on one connection</li>
 *   <li>Large Arrow result streaming over HTTP/2 flow control</li>
 *   <li>Arrow data ingestion over HTTP/2</li>
 *   <li>Content-type negotiation (Arrow vs TSV) over HTTP/2</li>
 *   <li>Arrow compression headers over HTTP/2</li>
 *   <li>Custom request headers (fetch_size, database) forwarded correctly</li>
 *   <li>Query cancellation over HTTP/2</li>
 *   <li>Health endpoint over HTTP/2</li>
 * </ul>
 */
public class HttpServerHttp2Test extends HttpServerTestBase {

    // -----------------------------------------------------------------------
    // Setup / teardown
    // -----------------------------------------------------------------------

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initPort();
        startServer(); // TLS disabled by default → h2c mode

        // Force the shared client to HTTP/2 (overrides initClient() default)
        client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();

        installArrowExtension();

        // Shared table used by several tests
        ConnectionPool.execute("CREATE TABLE IF NOT EXISTS h2_test (id INTEGER, name VARCHAR, score DOUBLE)");
        ConnectionPool.execute("INSERT INTO h2_test VALUES (1,'Alice',9.5),(2,'Bob',8.0),(3,'Carol',7.5)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS h2_test");
        cleanupWarehouse();
    }

    // -----------------------------------------------------------------------
    // 1. Protocol version negotiation
    // -----------------------------------------------------------------------

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
     * POST queries must also be served over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testPostQueryProtocolIsHttp2() throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest("SELECT 1"));
        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "POST /v1/query: server must respond in HTTP/2 binary framing (h2c Prior Knowledge)");
        assertEquals(200, response.statusCode());
    }

    /**
     * The login endpoint must also use HTTP/2 (Prior Knowledge).
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testLoginEndpointProtocolIsHttp2() throws Exception {
        var body = objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "POST /v1/login: server must respond in HTTP/2 binary framing (h2c Prior Knowledge)");
        assertEquals(200, response.statusCode());
    }

    /**
     * The health-check endpoint must also use HTTP/2 (Prior Knowledge).
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHealthCheckProtocolIsHttp2() throws Exception {
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/health"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "GET /health: server must respond in HTTP/2 binary framing (h2c Prior Knowledge)");
        assertEquals(200, response.statusCode());
    }

    // -----------------------------------------------------------------------
    // 2. Error responses preserve HTTP/2
    // -----------------------------------------------------------------------

    /**
     * A request without an Authorization header must get HTTP 401 — over HTTP/2.
     * Error paths must not fall back to HTTP/1.1.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testUnauthorizedResponseIsHttp2() throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest("SELECT 1"));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "401 response must still be served over HTTP/2");
        assertEquals(401, response.statusCode());
    }

    /**
     * A SQL syntax error must return HTTP 400 — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testSqlErrorResponseIsHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT FROM"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                "400 SQL error must still be served over HTTP/2");
        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("Error"),
                "Error body should mention Error: " + response.body());
    }

    /**
     * An invalid Arrow compression codec must return HTTP 400 — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testInvalidCompressionErrorIsHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1"))
                .GET()
                .header(HEADER_ARROW_COMPRESSION, "bad_codec")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(400, response.statusCode());
    }

    // -----------------------------------------------------------------------
    // 3. Data correctness over HTTP/2
    // -----------------------------------------------------------------------

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
     * POST query: verify the Arrow IPC response is both correct and served over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testPostQueryDataCorrectnessOverHttp2() throws Exception {
        var query = "SELECT * FROM h2_test ORDER BY id";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    // -----------------------------------------------------------------------
    // 4. JWT authentication flow over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * Full authentication flow: login to get a JWT token, then use it on a query.
     * Both requests must use HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testFullJwtAuthFlowOverHttp2() throws Exception {
        // Login
        var loginBody = objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"));
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(loginBody))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var loginResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, loginResponse.version(), "Login must use HTTP/2");
        assertEquals(200, loginResponse.statusCode());
        var jwt = objectMapper.readValue(loginResponse.body(), LoginResponse.class);
        assertNotNull(jwt.accessToken());

        // Query with the obtained token
        var query = "SELECT * FROM h2_test ORDER BY id";
        var queryBody = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var queryRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(queryBody))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        var queryResponse = client.send(queryRequest, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, queryResponse.version(), "Authenticated query must use HTTP/2");
        assertEquals(200, queryResponse.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(queryResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    /**
     * An expired / invalid Bearer token must return HTTP 401 — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testInvalidTokenIsRejectedOverHttp2() throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest("SELECT 1"));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer invalid.token.here")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(401, response.statusCode());
    }

    // -----------------------------------------------------------------------
    // 5. HTTP/2 multiplexing — concurrent streams on one connection
    // -----------------------------------------------------------------------

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
     * Multiplexed ingestion: concurrent Arrow POST ingestion requests must all
     * be served over HTTP/2 and all succeed.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMultiplexedConcurrentIngestion() throws Exception {
        final int concurrency = 20;
        var path = "h2_concurrent_ingest";
        Files.createDirectories(Path.of(ingestionPath, path));

        byte[] arrowBytes = buildArrowBytes("SELECT generate_series, generate_series a FROM generate_series(5)");

        var executor = Executors.newFixedThreadPool(concurrency);
        var token = getJWTToken();
        var http2Count = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    var request = HttpRequest.newBuilder(
                                    URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + path))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                            .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                            .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + token)
                            .build();
                    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertEquals(200, response.statusCode());
                    if (response.version() == HttpClient.Version.HTTP_2) {
                        http2Count.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertEquals(concurrency, http2Count.get(),
                "All concurrent ingestion requests must be served over HTTP/2");
    }

    // -----------------------------------------------------------------------
    // 6. Large result streaming over HTTP/2 flow control
    // -----------------------------------------------------------------------

    /**
     * Stream a large Arrow result over HTTP/2. This exercises HTTP/2 flow-control
     * and multi-batch Arrow IPC framing simultaneously.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testLargeArrowResultStreamingOverHttp2() throws Exception {
        var query = "SELECT * FROM generate_series(100000) ORDER BY 1";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            long rowCount = 0;
            while (reader.loadNextBatch()) {
                rowCount += reader.getVectorSchemaRoot().getRowCount();
            }
            assertEquals(100001, rowCount, "All 100 000 rows must arrive over HTTP/2");
        }
    }

    // -----------------------------------------------------------------------
    // 7. Data ingestion over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * POST Arrow IPC data to /v1/ingest over HTTP/2 and verify the data landed.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testIngestionOverHttp2() throws Exception {
        var path = "h2_ingest_basic";
        Files.createDirectories(Path.of(ingestionPath, path));
        byte[] arrowBytes = buildArrowBytes("SELECT * FROM generate_series(10)");

        var request = authenticatedRequestBuilder(
                URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + path))
                .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());

        var count = ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s/%s/*.parquet')".formatted(ingestionPath, path),
                Long.class);
        assertEquals(11L, count, "All ingested rows must be visible after HTTP/2 ingest");
    }

    // -----------------------------------------------------------------------
    // 8. Content-type negotiation over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * Requesting TSV via the Accept header must return TSV — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testTsvContentNegotiationOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT * FROM h2_test ORDER BY id"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        var contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("text/tab-separated-values"),
                "TSV content-type expected but got: " + contentType);
        var lines = response.body().split("\n");
        assertEquals(4, lines.length, "Header + 3 data rows expected");
        assertEquals("id\tname\tscore", lines[0]);
    }

    /**
     * Default Accept header (no explicit Accept) must return Arrow — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testDefaultArrowResponseOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS n"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        assertFalse(response.headers().firstValue("Content-Type")
                        .orElse("").contains("text/tab-separated-values"),
                "No Accept header should return Arrow, not TSV");
    }

    // -----------------------------------------------------------------------
    // 9. Arrow compression over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * ZSTD-compressed Arrow responses must still be served over HTTP/2 and must
     * be readable as valid Arrow IPC.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testZstdCompressionOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT * FROM h2_test ORDER BY id"))
                .GET()
                .header(HEADER_ARROW_COMPRESSION, "zstd")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            assertTrue(reader.loadNextBatch(), "Compressed Arrow must have at least one batch");
        }
    }

    // -----------------------------------------------------------------------
    // 10. Custom request headers forwarded correctly over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * The fetch_size header must be respected and the response served over HTTP/2.
     * A small batch size forces multiple Arrow IPC batches.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testFetchSizeHeaderOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT * FROM generate_series(100) ORDER BY 1"))
                .GET()
                .header(HEADER_FETCH_SIZE, "10")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            int batchCount = 0;
            long totalRows = 0;
            while (reader.loadNextBatch()) {
                batchCount++;
                totalRows += reader.getVectorSchemaRoot().getRowCount();
            }
            assertEquals(101, totalRows, "All rows must arrive");
            assertTrue(batchCount > 1, "Small fetch_size must produce multiple batches, got: " + batchCount);
        }
    }

    // -----------------------------------------------------------------------
    // 11. Query cancellation over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * Fire a long-running query and cancel it — both requests over HTTP/2.
     * The cancel must succeed and the query must be interrupted.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testCancelQueryOverHttp2() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        String encodedQuery = URLEncoder.encode(LONG_RUNNING_QUERY, StandardCharsets.UTF_8);
        var queryRequest = HttpRequest.newBuilder(
                URI.create(baseUrl + "/v1/query?q=%s&id=%s".formatted(encodedQuery, 99L)))
                .GET()
                .header("Accept", HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .build();
        var queryFuture = client.sendAsync(queryRequest, HttpResponse.BodyHandlers.ofString());

        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
                var cancelBody = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 99L));
                var cancelRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/cancel"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(cancelBody))
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header("Content-Type", "application/json")
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build();
                return client.send(cancelRequest, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertEquals(HttpClient.Version.HTTP_2, cancelResp.version(),
                "Cancel response must use HTTP/2");
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()),
                "Cancel should return 200/202/409, got: " + cancelResp.statusCode());

        var queryResp = queryFuture.get(15, TimeUnit.SECONDS);
        assertEquals(HttpClient.Version.HTTP_2, queryResp.version(),
                "Cancelled query response must use HTTP/2");
        var body = queryResp.body().toLowerCase();
        assertTrue(body.contains("cancel") || body.contains("interrupted") || queryResp.statusCode() != 200,
                "Query must be cancelled or fail, status=" + queryResp.statusCode());
    }

    // -----------------------------------------------------------------------
    // 12. All endpoints negotiate HTTP/2
    // -----------------------------------------------------------------------

    /**
     * Smoke-test every major endpoint to confirm HTTP/2 is negotiated regardless
     * of endpoint — not just the query path.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testAllEndpointsUseHttp2() throws Exception {
        var token = getJWTToken();
        var auth = "Bearer " + token;

        // GET /health (no auth needed)
        assertHttp2(client.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString()), "/health");

        // POST /v1/login
        assertHttp2(client.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(
                                objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                        .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                        .build(),
                HttpResponse.BodyHandlers.ofString()), "/v1/login");

        // GET /v1/query
        assertHttp2(client.send(
                HttpRequest.newBuilder(uriForQuery("SELECT 42"))
                        .GET()
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build(),
                HttpResponse.BodyHandlers.ofString()), "/v1/query GET");

        // POST /v1/query
        assertHttp2(client.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(
                                objectMapper.writeValueAsBytes(new QueryRequest("SELECT 42"))))
                        .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build(),
                HttpResponse.BodyHandlers.ofString()), "/v1/query POST");
    }

    // -----------------------------------------------------------------------
    // 13. Request pipelining / rapid sequential requests
    // -----------------------------------------------------------------------

    /**
     * Many sequential requests over the same HTTP/2 connection must all succeed.
     * This verifies that stream state is correctly reset between requests
     * (no lingering half-closed streams from previous responses).
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testRapidSequentialRequestsOverSameHttp2Connection() throws Exception {
        var token = getJWTToken();
        for (int i = 0; i < 20; i++) {
            var request = HttpRequest.newBuilder(uriForQuery("SELECT " + i + " AS n"))
                    .GET()
                    .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + token)
                    .build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(HttpClient.Version.HTTP_2, response.version(),
                    "Request #" + i + " must use HTTP/2");
            assertEquals(200, response.statusCode(),
                    "Request #" + i + " must succeed");
        }
    }

    // -----------------------------------------------------------------------
    // 14. Mixed async / sync multiplexing
    // -----------------------------------------------------------------------

    /**
     * Mix sendAsync and send over the same HTTP/2 client to exercise the
     * multiplexer with overlapping stream lifetimes.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMixedAsyncSyncMultiplexing() throws Exception {
        var token = getJWTToken();
        var auth = "Bearer " + token;

        // Start several async requests
        List<CompletableFuture<HttpResponse<String>>> asyncFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            var request = HttpRequest.newBuilder(uriForQuery("SELECT " + i + " AS async_n"))
                    .GET()
                    .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                    .build();
            asyncFutures.add(client.sendAsync(request, HttpResponse.BodyHandlers.ofString()));
        }

        // Interleave synchronous requests while async ones are in-flight
        for (int i = 0; i < 5; i++) {
            var request = HttpRequest.newBuilder(uriForQuery("SELECT " + i + " AS sync_n"))
                    .GET()
                    .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                    .build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(HttpClient.Version.HTTP_2, response.version());
            assertEquals(200, response.statusCode());
        }

        // Collect async results
        for (var f : asyncFutures) {
            var response = f.get(15, TimeUnit.SECONDS);
            assertEquals(HttpClient.Version.HTTP_2, response.version());
            assertEquals(200, response.statusCode());
        }
    }

    // -----------------------------------------------------------------------
    // 15. Empty result set over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * An Arrow IPC response for a query that returns zero rows must still be
     * a valid Arrow stream served over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testEmptyResultSetOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(
                uriForQuery("SELECT * FROM h2_test WHERE id = -999"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            long rowCount = 0;
            while (reader.loadNextBatch()) {
                rowCount += reader.getVectorSchemaRoot().getRowCount();
            }
            assertEquals(0, rowCount, "Empty result must have 0 rows");
        }
    }

    // -----------------------------------------------------------------------
    // 16. SET statement over HTTP/2
    // -----------------------------------------------------------------------

    /**
     * DuckDB SET statements must succeed and return an empty Arrow stream — over HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testSetStatementOverHttp2() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SET enable_progress_bar = false"))
                .GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(HttpClient.Version.HTTP_2, response.version());
        assertEquals(200, response.statusCode());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            while (reader.loadNextBatch()) {
                // just drain
            }
        }
    }

    // -----------------------------------------------------------------------
    // 17. Backward compatibility — HTTP/1.1 client against HTTP/2 server
    // -----------------------------------------------------------------------

    /**
     * An HTTP/1.1 client must still get correct responses from the HTTP/2-capable server.
     *
     * <p>This is the backward-compatibility test: the server must serve both HTTP/1.1
     * and HTTP/2 clients from the same listener. The response version must be HTTP_1_1
     * (the server negotiates down to what the client supports) and the data must be
     * identical to what an HTTP/2 client receives.
     *
     * <p>This test deliberately creates a separate {@link HttpClient} with
     * {@link HttpClient.Version#HTTP_1_1} to avoid contaminating the shared HTTP/2
     * client used by all other tests.
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

        // Server must respond using HTTP/1.1 (negotiated down to match the client)
        assertEquals(HttpClient.Version.HTTP_1_1, response.version(),
                "HTTP/1.1 client must get an HTTP/1.1 response (server negotiates down)");
        assertEquals(200, response.statusCode());

        // Data must be identical to what an HTTP/2 client gets
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(response.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    /**
     * HTTP/1.1 client errors (bad SQL) must also be handled correctly.
     * The 400 status must arrive over HTTP/1.1, not HTTP/2.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHttp11ClientSqlErrorIsHttp11() throws Exception {
        var http11Client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        var token = getJWTToken();
        var request = HttpRequest.newBuilder(uriForQuery("SELECT FROM"))
                .GET()
                .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + token)
                .build();
        var response = http11Client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(HttpClient.Version.HTTP_1_1, response.version(),
                "HTTP/1.1 client error response must stay on HTTP/1.1");
        assertEquals(400, response.statusCode());
        assertTrue(response.body().contains("Error"));
    }

    /**
     * Concurrent HTTP/1.1 and HTTP/2 clients hitting the same server simultaneously.
     * Each must get responses in their own protocol — the server must not mix them up.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHttp11AndHttp2ConcurrentlyOnSameServer() throws Exception {
        var http11Client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();

        var token = getJWTToken();
        var auth = "Bearer " + token;

        // Fire HTTP/1.1 and HTTP/2 requests at the same time
        var http2Future = client.sendAsync(
                HttpRequest.newBuilder(uriForQuery("SELECT 2 AS v"))
                        .GET()
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        var http11Future = http11Client.sendAsync(
                HttpRequest.newBuilder(uriForQuery("SELECT 1 AS v"))
                        .GET()
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        var http2Resp = http2Future.get(15, TimeUnit.SECONDS);
        var http11Resp = http11Future.get(15, TimeUnit.SECONDS);

        assertEquals(HttpClient.Version.HTTP_2, http2Resp.version(),
                "HTTP/2 client must get HTTP/2 response");
        assertEquals(200, http2Resp.statusCode());

        assertEquals(HttpClient.Version.HTTP_1_1, http11Resp.version(),
                "HTTP/1.1 client must get HTTP/1.1 response");
        assertEquals(200, http11Resp.statusCode());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }

    /** Assert that a response uses HTTP/2, printing the endpoint name on failure. */
    private static void assertHttp2(HttpResponse<?> response, String endpoint) {
        assertEquals(HttpClient.Version.HTTP_2, response.version(),
                endpoint + " must be served over HTTP/2");
    }

    /** Serialize the result of {@code query} into Arrow IPC bytes. */
    private static byte[] buildArrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return baos.toByteArray();
        }
    }
}
