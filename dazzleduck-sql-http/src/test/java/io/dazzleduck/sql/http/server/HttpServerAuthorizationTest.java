package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import io.helidon.http.HeaderNames;
import io.helidon.http.HeaderValues;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Create authorization server
 * Create tables. Insert some data
 * Check if the filters are applied when getStream is called.
 */
public class HttpServerAuthorizationTest extends HttpServerTestBase {

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer(
                "--conf", "dazzleduck_server.access_mode=RESTRICTED",
                "--conf", "dazzleduck_server.http.%s=jwt".formatted(ConfigUtils.AUTHENTICATION_KEY));
        ConnectionPool.execute("CREATE TABLE auth_test(id INTEGER, name STRING, city STRING, age INTEGER)");
        ConnectionPool.execute("INSERT INTO auth_test VALUES (1, 'shivam', 'chhindwara', 21), (2, 'hariom', 'delhi', 22), (3, 'piyush', 'bhopal', 21)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS auth_test");
        cleanupWarehouse();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryWithClaimsFilter() throws Exception {
        var claims = Map.of(
                "filter", "id = 1",
                "table", "auth_test",
                "path", warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            var expectedSql = "select * from auth_test where (id = 1)";
            TestUtils.isEqual(expectedSql, allocator, reader);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testUnauthorizedMissingTableClaim() throws Exception {
        var claims = Map.of(
                "filter", "id = 1",
                "path", warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        assertEquals(500, inputStreamResponse.statusCode());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Disabled("Column-level authorization not yet implemented")
    public void testColumnLevelAuthorization() throws Exception {
        var claims = Map.of(
                // "columns", List.of("id", "name"),
                "table", "auth_test",
                "path", warehousePath
        );
        var jwtResponse = loginWithClaims(claims);
        assertEquals(200, jwtResponse.statusCode());
        var jwt = objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
        var inputStreamResponse = query("select * from auth_test", jwt);
        try (var allocator = new RootAllocator();
             ArrowReader reader = new ArrowStreamReader(inputStreamResponse.body(), allocator)) {
            var expectedSql = "select id, name from auth_test";
            TestUtils.isEqual(expectedSql, allocator, reader);
        }
    }

    /**
     * ======== HELPER METHODS ========
     */
    // LOGIN (/v1/login) with claims
    private HttpResponse<String> loginWithClaims(Map<String, String> claims) throws Exception {
        var loginRequest = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", claims))
                ))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        return client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
    }

    // QUERY (/v1/query) with sql & jwt
    private HttpResponse<InputStream> query(String sql, LoginResponse jwt) throws Exception {
        var body = objectMapper.writeValueAsBytes(new QueryRequest(sql));
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .header(HeaderNames.AUTHORIZATION.defaultCase(), jwt.tokenType() + " " + jwt.accessToken())
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }
}
