package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ConfigConstants;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.common.Headers.*;
import static org.junit.jupiter.api.Assertions.*;

public class HttpServerJwtTest extends HttpServerTestBase {

    @BeforeAll
    public static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer("--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigConstants.AUTHENTICATION_KEY));
        installArrowExtension();
    }

    @AfterAll
    public static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    @Test
    public void testQueryWithJwtExpectUnauthorized() throws IOException, InterruptedException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(401, inputStreamResponse.statusCode());
    }

    @Test
    public void testQueryWithJwtExpect() throws IOException, InterruptedException, SQLException {
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testWithDuckDBAuthorized() throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var httpAuthSql = "CREATE SECRET http_auth (\n" +
                "    TYPE http,\n" +
                "    EXTRA_HTTP_HEADERS MAP {\n" +
                "        'Authorization': '" + jwt.tokenType() + " " + jwt.accessToken() + "'\n" +
                "    }\n" +
                ")";

        String viewSql = "select * from read_arrow(concat('%s/v1/query?q=',url_encode('select 1')))".formatted(baseUrl);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
        ConnectionPool.execute(httpAuthSql);
        ConnectionPool.execute(viewSql);
        ConnectionPool.execute("DROP SECRET http_auth");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCancelWithGet() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        String q = URLEncoder.encode(LONG_RUNNING_QUERY, StandardCharsets.UTF_8);
        var query = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query?q=%s&id=%s".formatted(q, 11L)))
                .GET().header("Accept", HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();

        var queryFuture = client.sendAsync(query, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
                var body = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 11L));
                var cancel = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/cancel"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header("Content-Type", "application/json")
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();
                return client.send(cancel, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()));

        var queryResp = queryFuture.get(15, TimeUnit.SECONDS);
        var body = queryResp.body().toLowerCase();
        assertTrue(body.contains("cancel") || body.contains("interrupted") || queryResp.statusCode() != 200);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCancelWithPost() throws Exception {
        var jwt = login();
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        var body = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 12L));
        var query = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Accept", HeaderValues.ACCEPT_JSON.values())
                .header("Content-Type", "application/json")
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();

        var queryFuture = client.sendAsync(query, HttpResponse.BodyHandlers.ofString());
        var cancelFuture = CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
                var cancelBody = objectMapper.writeValueAsBytes(new QueryRequest(LONG_RUNNING_QUERY, 12L));
                var cancel = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/cancel"))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(cancelBody))
                        .header("Accept", HeaderValues.ACCEPT_JSON.values())
                        .header("Content-Type", "application/json")
                        .header(HeaderNames.AUTHORIZATION.defaultCase(), auth).build();
                return client.send(cancel, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        var cancelResp = cancelFuture.get(5, TimeUnit.SECONDS);
        assertTrue(Set.of(200, 202, 409).contains(cancelResp.statusCode()));

        var queryResp = queryFuture.get(15, TimeUnit.SECONDS);
        var respBody = queryResp.body().toLowerCase();
        assertTrue(respBody.contains("cancel") || respBody.contains("interrupted") || queryResp.statusCode() != 200);
    }

    @Test
    public void testIngestionWithJwt() throws Exception {
        var path = "secure";
        var jwt = login(serverPort, new LoginRequest("admin", "admin",
                Map.of(QUERY_PARAMETER_INGESTION_QUEUE, path)));
        String auth = jwt.tokenType() + " " + jwt.accessToken();

        String query = "select * from generate_series(9)";
        byte[] arrowBytes;
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
            arrowBytes = baos.toByteArray();
        }


        Files.createDirectories(Path.of(ingestionPath, path));
        var authorizedReq = HttpRequest.newBuilder(
                        URI.create(baseUrl + "/v1/ingest?ingestion_queue=%s".formatted(path)))
                .POST(HttpRequest.BodyPublishers.ofByteArray(arrowBytes))
                .header("Content-Type", ContentTypes.APPLICATION_ARROW)
                .header(HeaderNames.AUTHORIZATION.defaultCase(), auth)
                .build();

        var authorizedResp = client.send(authorizedReq, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, authorizedResp.statusCode(),
                "Valid JWT token should allow ingestion");

        long count = ConnectionPool.collectFirst(
                "select count(*) from read_parquet('%s/%s/*.parquet')".formatted(ingestionPath, path),
                Long.class);

        assertEquals(10, count);
    }
}
