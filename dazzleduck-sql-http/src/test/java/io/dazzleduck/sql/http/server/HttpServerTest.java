package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpServerTest {
    static HttpClient client;
    static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    public static void setup() throws IOException, NoSuchAlgorithmException {
        String[] args1 = {"--conf", "port=8080"};
        Main.main(args1);
        client = HttpClient.newHttpClient();
        String[] args = {"--conf", "port=8081", "--conf", "auth=jwt"};
        Main.main(args);
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
    }

    @Test
    public void testQueryWithPost() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testQueryWithGet() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var urlEncode = URLEncoder.encode(query, StandardCharsets.UTF_8);
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/query?q=" + urlEncode))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testQueryWithJwtExpectUnauthorized() throws IOException, InterruptedException, SQLException {
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8081/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(401, inputStreamResponse.statusCode());
    }

    @Test
    public void testQueryWithJwtExpect() throws IOException, InterruptedException, SQLException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:8081/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        var jwt = jwtResponse.body();
        var query = "select * from generate_series(10) order by 1";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8081/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), "Bearer " + jwt)
                .build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            TestUtils.isEqual(query, allocator, reader);
        }
    }

    @Test
    public void testWithDuckDB() throws IOException, InterruptedException {
        String viewSql = "select * from read_arrow(concat('http://localhost:8080/query?q=',url_encode('select 1')))";

        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testWithDuckDBAuthorized() throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create("http://localhost:8081/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        var httpAuthSql = "CREATE SECRET http_auth (\n" +
                "    TYPE http,\n" +
                "    EXTRA_HTTP_HEADERS MAP {\n" +
                "        'Authorization': 'Bearer " + jwtResponse.body() + "'\n" +
                "    }\n" +
                ")";

        String viewSql = "select * from read_arrow(concat('http://localhost:8081/query?q=',url_encode('select 1')))";
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
        ConnectionPool.execute(httpAuthSql);
        ConnectionPool.execute(viewSql);
    }

    @Test
    public void testLogin() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginObject("admin", "admin"))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
        System.out.println(inputStreamResponse.body());
    }

    @Test
    public void testPlanning() throws IOException, InterruptedException {
        String query = "select count(*) from read_parquet('example/hive_table/*/*/*.parquet', hive_types = {'dt': DATE, 'p': VARCHAR})";
        var body = objectMapper.writeValueAsBytes(new QueryObject(query));
        var request = HttpRequest.newBuilder(URI.create("http://localhost:8080/plan"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        var res = objectMapper.readValue(inputStreamResponse.body(), Split[].class);
        assertEquals(1, res.length);
    }
}