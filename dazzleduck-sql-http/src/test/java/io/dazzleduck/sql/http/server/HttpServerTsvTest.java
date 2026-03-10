package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class HttpServerTsvTest extends HttpServerTestBase {

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
        installArrowExtension();
        ConnectionPool.execute("CREATE TABLE tsv_test(id INTEGER, name VARCHAR, score INTEGER)");
        ConnectionPool.execute("INSERT INTO tsv_test VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS tsv_test");
        cleanupWarehouse();
    }

    // ==================== QUERY OUTPUT TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryTsvWithGet() throws Exception {
        var query = "SELECT * FROM tsv_test ORDER BY id";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET()
                .header("Accept", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals(4, lines.length); // header + 3 data rows
        assertEquals("id\tname\tscore", lines[0]);
        assertEquals("1\tAlice\t10", lines[1]);
        assertEquals("2\tBob\t20", lines[2]);
        assertEquals("3\tCarol\t30", lines[3]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryTsvWithPost() throws Exception {
        var query = "SELECT * FROM tsv_test ORDER BY id";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Accept", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals(4, lines.length);
        assertEquals("id\tname\tscore", lines[0]);
        assertEquals("1\tAlice\t10", lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryTsvResponseContentType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS id"))
                .GET()
                .header("Accept", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        var contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("text/tab-separated-values"),
                "Expected TSV Content-Type but got: " + contentType);
        assertTrue(contentType.contains("utf-8"),
                "Expected charset=utf-8 but got: " + contentType);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryTsvSingleColumn() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 42 AS answer"))
                .GET()
                .header("Accept", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals(2, lines.length);
        assertEquals("answer", lines[0]);
        assertEquals("42", lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryTsvEmptyResultHasHeader() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT * FROM tsv_test WHERE id = -999"))
                .GET()
                .header("Accept", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String body = response.body().trim();
        // Should have the header row even with no data rows
        assertEquals("id\tname\tscore", body);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryWithoutAcceptHeaderReturnsArrow() throws Exception {
        // Omitting Accept header should fall back to Arrow (existing behaviour unchanged)
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS id"))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertFalse(response.headers().firstValue("Content-Type")
                        .orElse("").contains("text/tab-separated-values"),
                "No Accept header should not return TSV Content-Type");
    }

    // ==================== INGESTION TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionTsv() throws Exception, SQLException {
        var queue = "tsv_ingest_basic";
        Files.createDirectories(Path.of(ingestionPath, queue));
        String tsv = "id\tname\n1\tAlice\n2\tBob\n3\tCarol\n";

        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString(tsv, StandardCharsets.UTF_8))
                .header("Content-Type", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        var count = ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s/%s/*.parquet')".formatted(ingestionPath, queue), Long.class);
        assertEquals(3L, count);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionTsvWithCharsetParam() throws Exception, SQLException {
        // Content-Type: text/tab-separated-values; charset=utf-8 must also be accepted
        var queue = "tsv_ingest_charset";
        Files.createDirectories(Path.of(ingestionPath, queue));
        String tsv = "id\tname\n10\tDave\n20\tEve\n";

        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString(tsv, StandardCharsets.UTF_8))
                .header("Content-Type", ContentTypes.TEXT_TSV_UTF8)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        var count = ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s/%s/*.parquet')".formatted(ingestionPath, queue), Long.class);
        assertEquals(2L, count);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionTsvWithNullFields() throws Exception, SQLException {
        // Empty tab fields become null Arrow values
        var queue = "tsv_ingest_nulls";
        Files.createDirectories(Path.of(ingestionPath, queue));
        String tsv = "id\tname\tvalue\n1\tAlice\t\n2\t\t99\n";

        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString(tsv, StandardCharsets.UTF_8))
                .header("Content-Type", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        var count = ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s/%s/*.parquet')".formatted(ingestionPath, queue), Long.class);
        assertEquals(2L, count);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionTsvValuesAreCorrect() throws Exception, SQLException {
        var queue = "tsv_ingest_values";
        Files.createDirectories(Path.of(ingestionPath, queue));
        String tsv = "code\tlabel\n42\thello\n99\tworld\n";

        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString(tsv, StandardCharsets.UTF_8))
                .header("Content-Type", ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        // All TSV columns land as VARCHAR — use DuckDB to verify the values exist
        var found = ConnectionPool.collectFirst(
                "SELECT count(*) FROM read_parquet('%s/%s/*.parquet') WHERE code='42' AND label='hello'"
                        .formatted(ingestionPath, queue), Long.class);
        assertEquals(1L, found);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionWrongContentTypeReturns415() throws Exception {
        var queue = "tsv_wrong_ct";
        Files.createDirectories(Path.of(ingestionPath, queue));

        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString("id\tname\n1\tAlice\n"))
                .header("Content-Type", "text/plain")
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(415, response.statusCode());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIngestionMissingContentTypeReturns415() throws Exception {
        var queue = "tsv_no_ct";
        Files.createDirectories(Path.of(ingestionPath, queue));

        // HttpRequest.BodyPublishers.ofString sets no Content-Type by default
        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/ingest?ingestion_queue=" + queue))
                .POST(HttpRequest.BodyPublishers.ofString("id\tname\n1\tAlice\n"))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(415, response.statusCode());
    }

    // ==================== HELPERS ====================

    private URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }
}
