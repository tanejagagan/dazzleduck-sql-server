package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderValues;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
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
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
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
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
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
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
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
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
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
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
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
        // Omitting Accept header should fall back to Arrow (existing behavior unchanged)
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS id"))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertFalse(response.headers().firstValue("Content-Type")
                        .orElse("").contains("text/tab-separated-values"),
                "No Accept header should not return TSV Content-Type");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testQueryMillionRowsSingleColumn() throws Exception {
        var query = "SELECT generate_series FROM generate_series(1, 1000000)";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals(1000001, lines.length); // header + 1M rows
        assertEquals("generate_series", lines[0]);
        assertEquals("1", lines[1]);
        assertEquals("1000000", lines[1000000]);
    }

    // ==================== HELPERS ====================

    private URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }
}
