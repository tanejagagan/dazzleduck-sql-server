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

    // ==================== BASIC QUERY TESTS ====================

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

    // ==================== DATE / TIME / TIMESTAMP TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDateType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT CAST('2026-03-12' AS DATE) AS d"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("d", lines[0]);
        assertEquals("2026-03-12", lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTimeType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT CAST('12:30:45' AS TIME) AS t"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("t", lines[0]);
        assertTrue(lines[1].startsWith("12:30:45"), "Expected time starting with 12:30:45, got: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTimestampType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT CAST('2026-03-12 10:30:00' AS TIMESTAMP) AS ts"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("ts", lines[0]);
        assertTrue(lines[1].startsWith("2026-03-12"), "Expected timestamp starting with 2026-03-12, got: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTimestampTzType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT CAST('2026-03-12 10:30:00+00' AS TIMESTAMPTZ) AS tstz"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("tstz", lines[0]);
        assertTrue(lines[1].contains("2026-03-12"), "Expected TIMESTAMPTZ containing 2026-03-12, got: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testAllTemporalTypesInOneQuery() throws Exception {
        var sql = "SELECT " +
                "CAST('2026-03-12' AS DATE) AS d, " +
                "CAST('14:00:00' AS TIME) AS t, " +
                "CAST('2026-03-12 14:00:00' AS TIMESTAMP) AS ts, " +
                "CAST('2026-03-12 14:00:00+00' AS TIMESTAMPTZ) AS tstz";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("d\tt\tts\ttstz", lines[0]);
        String[] cols = lines[1].split("\t");
        assertEquals("2026-03-12", cols[0]);
        assertTrue(cols[1].startsWith("14:00"), "TIME: " + cols[1]);
        assertTrue(cols[2].startsWith("2026-03-12"), "TIMESTAMP: " + cols[2]);
        assertTrue(cols[3].contains("2026-03-12"), "TIMESTAMPTZ: " + cols[3]);
    }

    // ==================== NULL TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNullValues() throws Exception {
        var sql = "SELECT NULL::INTEGER AS i, NULL::VARCHAR AS s, NULL::DATE AS d, NULL::TIMESTAMP AS ts";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("i\ts\td\tts", lines[0]);
        assertEquals("\t\t\t", lines[1]); // all nulls → empty strings separated by tabs
    }

    // ==================== NUMERIC TYPES ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNumericTypes() throws Exception {
        var sql = "SELECT " +
                "CAST(1 AS TINYINT) AS ti, " +
                "CAST(2 AS SMALLINT) AS si, " +
                "CAST(3 AS INTEGER) AS i, " +
                "CAST(4 AS BIGINT) AS bi, " +
                "CAST(1.5 AS FLOAT) AS f, " +
                "CAST(2.5 AS DOUBLE) AS d, " +
                "CAST(3.14 AS DECIMAL(10,2)) AS dec, " +
                "true AS b";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("ti\tsi\ti\tbi\tf\td\tdec\tb", lines[0]);
        String[] cols = lines[1].split("\t");
        assertEquals("1", cols[0]);
        assertEquals("2", cols[1]);
        assertEquals("3", cols[2]);
        assertEquals("4", cols[3]);
        assertEquals("1.5", cols[4]);
        assertEquals("2.5", cols[5]);
        assertEquals("3.14", cols[6]);
        assertEquals("true", cols[7]);
    }

    // ==================== ARRAY / LIST TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testIntegerArray() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT [1, 2, 3] AS arr"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("arr", lines[0]);
        assertTrue(lines[1].contains("1"), "Array should contain 1: " + lines[1]);
        assertTrue(lines[1].contains("2"), "Array should contain 2: " + lines[1]);
        assertTrue(lines[1].contains("3"), "Array should contain 3: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testStringArray() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT ['a', 'b', 'c'] AS arr"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("arr", lines[0]);
        assertTrue(lines[1].contains("a"), "Array should contain a: " + lines[1]);
        assertTrue(lines[1].contains("b"), "Array should contain b: " + lines[1]);
        assertTrue(lines[1].contains("c"), "Array should contain c: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testArrayWithNulls() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT [1, NULL, 3] AS arr"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("arr", lines[0]);
        assertNotNull(lines[1]);
    }

    // ==================== STRUCT / NESTED TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testStructType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT {'name': 'Alice', 'age': 30} AS s"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("s", lines[0]);
        assertTrue(lines[1].contains("Alice"), "Struct should contain Alice: " + lines[1]);
        assertTrue(lines[1].contains("30"), "Struct should contain 30: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNestedStruct() throws Exception {
        var sql = "SELECT {'outer': {'inner': 42}} AS nested";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("nested", lines[0]);
        assertTrue(lines[1].contains("42"), "Nested struct should contain 42: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testStructWithTemporalFields() throws Exception {
        var sql = "SELECT {'d': CAST('2026-03-12' AS DATE), 'name': 'test'} AS s";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("s", lines[0]);
        assertNotNull(lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testArrayOfStructs() throws Exception {
        var sql = "SELECT [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}] AS arr";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("arr", lines[0]);
        assertTrue(lines[1].contains("1"), "Should contain 1: " + lines[1]);
        assertTrue(lines[1].contains("x"), "Should contain x: " + lines[1]);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMapType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT MAP {'key1': 1, 'key2': 2} AS m"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("m", lines[0]);
        assertTrue(lines[1].contains("key1"), "Map should contain key1: " + lines[1]);
        assertTrue(lines[1].contains("key2"), "Map should contain key2: " + lines[1]);
    }

    // ==================== MIXED TYPES ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMixedTypesInOneRow() throws Exception {
        var sql = "SELECT " +
                "42 AS i, " +
                "'hello' AS s, " +
                "true AS b, " +
                "CAST('2026-03-12' AS DATE) AS d, " +
                "[1,2,3] AS arr, " +
                "{'x': 10} AS st";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.TEXT_TSV)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        String[] lines = response.body().split("\n");
        assertEquals("i\ts\tb\td\tarr\tst", lines[0]);
        String[] cols = lines[1].split("\t");
        assertEquals("42", cols[0]);
        assertEquals("hello", cols[1]);
        assertEquals("true", cols[2]);
        assertEquals("2026-03-12", cols[3]);
        assertTrue(cols[4].contains("1"), "Array: " + cols[4]);
        assertTrue(cols[5].contains("10"), "Struct: " + cols[5]);
    }

    // ==================== HELPERS ====================

    private URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }
}
