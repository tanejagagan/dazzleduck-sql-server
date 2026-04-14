package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.common.NamedQueryParameterValidator;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.model.NamedQueryRequest;
import io.dazzleduck.sql.http.server.model.QueryMode;
import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link NamedQueryService}.
 *
 * <p>The server is started with {@code named_query_table=named_queries}. Each test that
 * needs a template inserts a row into that table before making the HTTP request.
 */
public class NamedQueryServiceTest extends HttpServerTestBase {

    private static final String NAMED_QUERIES_TABLE = "named_queries";
    private static final String ENDPOINT = "/v1/named-query";

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer(
                "--conf", "dazzleduck_server.named_query_table=" + NAMED_QUERIES_TABLE
        );
        installArrowExtension();

        // Create the named-query table and seed it with templates used by the tests
        ConnectionPool.execute(
                "CREATE TABLE IF NOT EXISTS " + NAMED_QUERIES_TABLE +
                " (name VARCHAR PRIMARY KEY, template VARCHAR, validators VARCHAR[]," +
                "  description VARCHAR, parameter_descriptions MAP(VARCHAR, VARCHAR))");
        ConnectionPool.executeBatch(new String[]{
                "DELETE FROM " + NAMED_QUERIES_TABLE,
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "('get_series', 'SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v'," +
                " NULL, 'Returns the first N integers', MAP {'limit': 'upper bound (exclusive)'})",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "('filter_series', 'SELECT * FROM generate_series(10) t(v) WHERE v > {{ min }}'," +
                " NULL, 'Filters integers above a threshold', MAP {'min': 'minimum value (exclusive)'})",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "('validated_query', 'SELECT * FROM generate_series({{ limit }}) t(v)'," +
                " ['" + RejectAllValidatorNamedQuery.class.getName() + "'], 'Always rejected by validator', NULL)",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "('multi_validator_query', 'SELECT * FROM generate_series({{ limit }}) t(v)'," +
                " ['" + RejectAllValidatorNamedQuery.class.getName() + "', '" + AnotherRejectValidatorNamedQuery.class.getName() + "']," +
                " 'Rejected by two validators', NULL)"
        });
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS " + NAMED_QUERIES_TABLE);
        cleanupWarehouse();
    }

    // -------------------------------------------------------------------------
    // Happy-path tests
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_templateRenderedAndExecuted() throws IOException, InterruptedException, SQLException {
        // Template: SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v
        var namedQuery = NamedQueryRequest.execute("get_series", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "Expected 200 for valid named query");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            String expected = "SELECT * FROM generate_series(5) t(v) ORDER BY v";
            TestUtils.isEqual(expected, allocator, reader);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_multipleParameters() throws IOException, InterruptedException, SQLException {
        // Template: SELECT * FROM generate_series(10) t(v) WHERE v > {{ min }}
        var namedQuery = NamedQueryRequest.execute("filter_series", Map.of("min", "7"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "Expected 200 for valid named query with parameters");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            String expected = "SELECT * FROM generate_series(10) t(v) WHERE v > 7";
            TestUtils.isEqual(expected, allocator, reader);
        }
    }

    // -------------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testUnknownNameReturns404() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("no_such_query", Map.of());
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode(), "Expected 404 when named query does not exist");
        assertNotNull(response.body());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testMissingNameReturns400() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest(null, Map.of(), QueryMode.EXECUTE);
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "Expected 400 when name is missing");
    }

    // -------------------------------------------------------------------------
    // Validator test — row in DB has a validator class that rejects everything
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testValidatorBlocksRequest() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("validated_query", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "Validator should cause HTTP 400");
        assertTrue(response.body().contains("Rejected by test validator"),
                "Response body should contain the validator message");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testAllValidatorsRunAndErrorsAreCombined() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("multi_validator_query", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "All validators should cause HTTP 400");
        assertTrue(response.body().contains("Rejected by test validator"),
                "Response should contain first validator message");
        assertTrue(response.body().contains("Rejected by another validator"),
                "Response should contain second validator message");
    }

    // -------------------------------------------------------------------------
    // EXPLAIN / EXPLAIN ANALYZE tests
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testExplainMode() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest("get_series", Map.of("limit", "5"), QueryMode.EXPLAIN);
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "EXPLAIN should return 200");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            // EXPLAIN returns a plan string — just verify we get at least one batch
            assertTrue(reader.loadNextBatch(), "EXPLAIN should return at least one batch");
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGenerateMode() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest("get_series", Map.of("limit", "5"), QueryMode.GENERATE);
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "GENERATE should return 200");
        assertEquals("SELECT * FROM generate_series(5) t(v) ORDER BY v", response.body().trim());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testExplainAnalyzeMode() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest("get_series", Map.of("limit", "5"), QueryMode.EXPLAIN_ANALYZE);
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "EXPLAIN ANALYZE should return 200");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            assertTrue(reader.loadNextBatch(), "EXPLAIN ANALYZE should return at least one batch");
        }
    }

    // -------------------------------------------------------------------------
    // List + single-query tests
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testListReturnsPaginatedItems() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "?offset=0&limit=10"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Expected 200 for list endpoint");

        Map<String, Object> page = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals(4, ((Number) page.get("total")).intValue(), "Expected total=4");
        assertEquals(0, ((Number) page.get("offset")).intValue());
        assertEquals(10, ((Number) page.get("limit")).intValue());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) page.get("items");
        assertEquals(4, items.size(), "All 4 queries fit within limit=10");

        // Names sorted alphabetically, only name + description present (no template, no validators)
        assertEquals("filter_series",         items.get(0).get("name"));
        assertEquals("get_series",            items.get(1).get("name"));
        assertEquals("multi_validator_query", items.get(2).get("name"));
        assertEquals("validated_query",       items.get(3).get("name"));
        assertFalse(items.get(0).containsKey("template"), "template must not appear in list");
        assertFalse(items.get(0).containsKey("validatorDescriptions"), "validators must not appear in list");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testListPaginationOffset() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "?offset=2&limit=2"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        Map<String, Object> page = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals(4, ((Number) page.get("total")).intValue());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) page.get("items");
        assertEquals(2, items.size(), "Offset=2, limit=2 should return items 3 and 4");
        assertEquals("multi_validator_query", items.get(0).get("name"));
        assertEquals("validated_query",       items.get(1).get("name"));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameReturnsFullObject() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/get_series"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Expected 200 for named query by name");

        Map<String, Object> info = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals("get_series",           info.get("name"));
        assertEquals("Returns the first N integers", info.get("description"));
        assertTrue(info.containsKey("parameterDescriptions"), "Full object must include parameterDescriptions");
        assertTrue(info.containsKey("validatorDescriptions"), "Full object must include validatorDescriptions");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameWithValidatorsReturnsDescriptions() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/validated_query"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        Map<String, Object> info = objectMapper.readValue(response.body(), new TypeReference<>() {});
        @SuppressWarnings("unchecked")
        List<String> descs = (List<String>) info.get("validatorDescriptions");
        assertNotNull(descs);
        assertEquals(1, descs.size());
        // RejectAllValidatorNamedQuery.description() is "" → falls back to class name
        assertNotNull(descs.get(0));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameUnknownReturns404() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/no_such_query"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }

    // -------------------------------------------------------------------------
    // Test validator implementations — must be public with a no-arg constructor
    // -------------------------------------------------------------------------

    /** A validator that always rejects — used only in tests. */
    public static class RejectAllValidatorNamedQuery implements NamedQueryParameterValidator {
        @Override
        public void validate(Map<String, String> parameters) throws ParameterValidationException {
            throw new ParameterValidationException("Rejected by test validator");
        }
    }

    /** A second validator that always rejects — used to verify multi-validator error collection. */
    public static class AnotherRejectValidatorNamedQuery implements NamedQueryParameterValidator {
        @Override
        public void validate(Map<String, String> parameters) throws ParameterValidationException {
            throw new ParameterValidationException("Rejected by another validator");
        }
    }
}
