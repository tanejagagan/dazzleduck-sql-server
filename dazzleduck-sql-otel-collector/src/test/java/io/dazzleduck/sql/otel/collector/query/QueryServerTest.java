package io.dazzleduck.sql.otel.collector.query;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Queries over HTTP against a DuckDB-file DuckLake catalog attached to the shared
 * {@link ConnectionPool} instance, as the collector's startup script leaves it.
 */
class QueryServerTest {

    private static final String CATALOG = "query_lake";

    @TempDir
    static Path tempDir;

    static QueryServer server;
    static SimpleMeterRegistry registry;
    static HttpClient client;

    @BeforeAll
    static void setUp() throws Exception {
        Path data = tempDir.resolve("data");
        Files.createDirectories(data);
        ConnectionPool.execute("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')"
                .formatted(tempDir.resolve("meta.ducklake"), CATALOG, data));
        ConnectionPool.execute(("CREATE TABLE %s.main.events AS SELECT range AS id, 'e' || range AS name, "
                + "DATE '2026-09-01' + (range %% 3)::INTEGER AS day, [range, range + 1] AS pair FROM range(3)").formatted(CATALOG));
        registry = new SimpleMeterRegistry();
        server = new QueryServer(new QuerySettings(true, "127.0.0.1", 0, 4, Duration.ofSeconds(2), 1000), registry);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void tearDown() throws Exception {
        server.close();
        ConnectionPool.execute("DETACH " + CATALOG);
    }

    private static HttpResponse<byte[]> get(String sql, String accept, String... headers) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:%d/v1/query?q=%s"
                .formatted(server.getPort(), URLEncoder.encode(sql, StandardCharsets.UTF_8)))).GET();
        if (accept != null) builder.header("Accept", accept);
        for (int i = 0; i < headers.length; i += 2) builder.header(headers[i], headers[i + 1]);
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String text(HttpResponse<byte[]> response) {
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private static long arrowRows(byte[] body) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             var reader = new ArrowStreamReader(new ByteArrayInputStream(body), allocator, CommonsCompressionFactory.INSTANCE)) {
            long rows = 0;
            while (reader.loadNextBatch()) rows += reader.getVectorSchemaRoot().getRowCount();
            return rows;
        }
    }

    @Test
    void tsvFromTheSharedCatalog() throws Exception {
        var response = get("SELECT id, name, day FROM %s.main.events ORDER BY id".formatted(CATALOG), "text/tab-separated-values");
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/tab-separated-values"));
        assertEquals("id\tname\tday\n0\te0\t2026-09-01\n1\te1\t2026-09-02\n2\te2\t2026-09-03\n", text(response));
    }

    @Test
    void jsonlViaPost() throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:%d/v1/query".formatted(server.getPort())))
                .header("Accept", "application/jsonl")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"query\": \"SELECT id, pair FROM %s.main.events WHERE id = 1\"}".formatted(CATALOG)))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("{\"id\":1,\"pair\":[1,2]}\n", response.body());
    }

    @Test
    void ndjsonIsAnAliasForJsonl() throws Exception {
        assertEquals("{\"n\":7}\n", text(get("SELECT 7 AS n", "application/x-ndjson")));
    }

    @Test
    void arrowIsTheDefaultAndZstdCompressed() throws Exception {
        var response = get("SELECT * FROM %s.main.events".formatted(CATALOG), null);
        assertEquals(200, response.statusCode());
        assertEquals("application/vnd.apache.arrow.stream", response.headers().firstValue("Content-Type").orElse(""));
        assertEquals(3, arrowRows(response.body()));
    }

    @Test
    void arrowWithoutCompression() throws Exception {
        var response = get("SELECT * FROM range(10)", null, "x-dd-arrow-compression", "none");
        assertEquals(200, response.statusCode());
        try (BufferAllocator allocator = new RootAllocator();
             var reader = new ArrowStreamReader(new ByteArrayInputStream(response.body()), allocator)) {
            assertTrue(reader.loadNextBatch(), "readable without a compression codec");
            assertEquals(10, reader.getVectorSchemaRoot().getRowCount());
        }
    }

    @Test
    void aLargeResultStreamsInFull() throws Exception {
        assertEquals(1_000_000, arrowRows(get("SELECT range AS id, range * 2 AS v FROM range(1000000)", null).body()));
    }

    @Test
    void writesToTablesAreRejected() throws Exception {
        var response = get("INSERT INTO %s.main.events VALUES (99, 'x', DATE '2026-01-01', [1, 2])".formatted(CATALOG), null);
        assertEquals(500, response.statusCode());
        assertTrue(text(response).contains("read-only"), text(response));
        assertEquals("3\n", text(get("SELECT count(*) AS n FROM %s.main.events".formatted(CATALOG), "text/tab-separated-values"))
                .lines().skip(1).map(l -> l + "\n").findFirst().orElse(""), "nothing was inserted");
    }

    @Test
    void aSlowQueryIsCancelledWithA504() throws Exception {
        long start = System.nanoTime();
        var response = get("SELECT sum(a.range * b.range) FROM range(200000) a, range(200000) b", null);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(504, response.statusCode(), text(response));
        assertTrue(elapsedMs < 10_000, "cancelled near the 2s timeout, took " + elapsedMs + "ms");
        assertNotNull(registry.find("dazzleduck.otel.query.duration").tag("outcome", "timeout").timer());
    }

    @Test
    void browserInitiatedRequestsAreRefused() throws Exception {
        // What a web page can make the developer's browser send (<img>, fetch, form post).
        assertEquals(403, get("SELECT 1", null, "Origin", "https://evil.example").statusCode());
        assertEquals(403, get("SELECT 1", null, "Sec-Fetch-Site", "cross-site").statusCode());
        assertEquals(403, get("SELECT 1", null, "Sec-Fetch-Site", "same-site").statusCode());
        // A URL the developer typed into the address bar, and command-line clients, are fine.
        assertEquals(200, get("SELECT 1", "text/tab-separated-values", "Sec-Fetch-Site", "none").statusCode());
        assertNotNull(registry.find("dazzleduck.otel.query.duration").tag("outcome", "forbidden").timer());
    }

    @Test
    void aNonLoopbackHostHeaderIsRefusedWhileBoundToLocalhost() throws Exception {
        // DNS rebinding: evil.example resolves to 127.0.0.1, so the request arrives with its Host.
        // java.net.http will not set Host, so this one goes over a raw socket.
        assertTrue(rawStatusLine("evil.example:" + server.getPort()).contains(" 403 "));
        assertTrue(rawStatusLine("localhost:" + server.getPort()).contains(" 200 "));
        assertTrue(rawStatusLine("[::1]:" + server.getPort()).contains(" 200 "));
    }

    private static String rawStatusLine(String hostHeader) throws Exception {
        try (var socket = new java.net.Socket("127.0.0.1", server.getPort())) {
            socket.getOutputStream().write(("GET /v1/query?q=SELECT%201 HTTP/1.1\r\nHost: " + hostHeader
                    + "\r\nAccept: text/tab-separated-values\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            return new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))
                    .readLine();
        }
    }

    @Test
    void closeCancelsQueriesThatAreStillRunning() throws Exception {
        // A long timeout, so only close() can stop the query.
        var own = new QueryServer(new QuerySettings(true, "127.0.0.1", 0, 2, Duration.ofMinutes(5), 1000), registry);
        own.start();
        var slow = client.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:%d/v1/query?q=%s".formatted(own.getPort(),
                        URLEncoder.encode("SELECT sum(a.range * b.range) FROM range(300000) a, range(300000) b", StandardCharsets.UTF_8))))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        long waitUntil = System.currentTimeMillis() + 10_000;
        while (own.runningQueries() == 0 && System.currentTimeMillis() < waitUntil) {
            Thread.sleep(20);
        }
        assertEquals(1, own.runningQueries(), "the slow query is executing");
        long start = System.nanoTime();
        own.close();
        long closeMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(closeMs < 8_000, "close() cancelled the running query instead of waiting it out: " + closeMs + "ms");
        var response = slow.get(10, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(500, response.statusCode(), "the cancelled query is answered, not left hanging: " + response.body());
    }

    @Test
    void badRequests() throws Exception {
        var missing = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:%d/v1/query".formatted(server.getPort()))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(400, missing.statusCode());
        assertEquals(400, get("SELECT 1", null, "x-dd-arrow-compression", "lz4").statusCode());
        var put = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:%d/v1/query?q=SELECT%%201".formatted(server.getPort())))
                .PUT(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(405, put.statusCode());
        var badSql = get("SELEKT 1", "text/tab-separated-values");
        assertEquals(500, badSql.statusCode());
        assertTrue(text(badSql).contains("syntax error"), text(badSql));
    }
}
