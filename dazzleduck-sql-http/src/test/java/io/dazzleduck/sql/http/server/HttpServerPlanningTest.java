package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestConstants;
import io.dazzleduck.sql.flight.server.StatementHandle;
import io.dazzleduck.sql.login.LoginRequest;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

public class HttpServerPlanningTest extends HttpServerTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer("--conf", "dazzleduck_server.access_mode=RESTRICTED");
        installArrowExtension();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @ParameterizedTest
    @ValueSource(strings = {TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanning(String query) throws IOException, InterruptedException {
        var loginResponse = login(new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(inputStreamResponse.body());
        System.out.println(inputStreamResponse);
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(1, res.length);
    }

    @Test
    public void testPrintPlaning() throws SQLException, IOException, InterruptedException {
        var loginResponse = login(new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        var secretQuery = """
                CREATE SECRET jwt_secret (
                    TYPE http,
                    BEARER_TOKEN '%s'
                )""".formatted(loginResponse.accessToken());
        try (Connection connection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ConnectionPool.execute(connection, secretQuery);
            var query = "%s where p='1'".formatted(TestConstants.SUPPORTED_HIVE_PATH_QUERY);
            var request = baseUrl + "/v1/plan?%s=1&q=".formatted(HEADER_SPLIT_SIZE);
            var toExecute = "SELECT splitSize FROM read_json(concat('%s', url_encode('%s')))".formatted(request, query.replaceAll("'", "''"));
            ConnectionPool.printResult(connection, allocator, toExecute);
            assertEquals(254, ConnectionPool.collectFirst(toExecute, Long.class));
            ConnectionPool.execute(connection, "DROP SECRET jwt_secret");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {TestConstants.SUPPORTED_HIVE_PATH_QUERY, TestConstants.SUPPORTED_AGGREGATED_HIVE_PATH_QUERY})
    public void testPlanningWithSmallPartition(String query) throws IOException, InterruptedException {
        var loginResponse = login(new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(3, res.length);
    }

    @Test
    public void testPlanningWithFilter() throws IOException, InterruptedException {
        var loginResponse = login(new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var filter = "WHERE dt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + filter));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), StatementHandle[].class);
        assertEquals(2, res.length);
    }

    @Test
    public void testPlanningWithError() throws IOException, InterruptedException {
        var loginResponse = login(new LoginRequest("admin", "admin",
                Map.of(HEADER_PATH, TestConstants.SUPPORTED_HIVE_PATH, HEADER_FUNCTION, "read_parquet")));
        String auth = loginResponse.tokenType() + " " + loginResponse.accessToken();
        var errorFilter = "WHEREdt = '2025-01-01'";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY + errorFilter));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HEADER_SPLIT_SIZE, "1")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(500, inputStreamResponse.statusCode());
        assertNotNull(inputStreamResponse.body());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("No way to slow down query processing - cancel happens too fast")
    public void testCancelWithPlanning() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();
        var planBody = objectMapper.writeValueAsBytes(new QueryRequest(TestConstants.SUPPORTED_HIVE_PATH_QUERY));
        var planReq = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(planBody))
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header("Content-Type", "application/json")
                .build();
        var planResp = client.send(planReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, planResp.statusCode());
        var handles = objectMapper.readValue(planResp.body(), StatementHandle[].class);
        assertNotNull(handles);
        assertTrue(handles.length > 0);

        String plannedQuery = handles[0].query();
        long id = handles[0].queryId();

        String qEnc = URLEncoder.encode(plannedQuery, StandardCharsets.UTF_8);
        var queryReq = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s&id=%s".formatted(qEnc, id)))
                .GET()
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .header("Accept", HeaderValues.ACCEPT_JSON.values())
                .build();
        client.sendAsync(queryReq, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(2000);
                var cancelBody = objectMapper.writeValueAsBytes(new QueryRequest(plannedQuery, id));
                var cancelReq = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/cancel"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(cancelBody))
                        .header("Content-Type", "application/json")
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                        .build();
                return client.send(cancelReq, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()), "unexpected cancel status");
    }
}
