package io.dazzleduck.sql.login;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.helidon.http.HeaderValues;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLoginService {
    // Test configuration
    private static final int TEST_PORT = 8080;
    private static final String BASE_URL = "http://localhost:" + TEST_PORT;
    private static final String LOGIN_ENDPOINT = BASE_URL + "/v1/login";

    static ObjectMapper objectMapper = new ObjectMapper();
    static HttpClient client;

    @BeforeAll
    public static void setup() throws Exception {
        var t = new Thread(() -> {
            try {
                Main.main(new String[0]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        Thread.sleep(500);
        client = HttpClient.newHttpClient();
    }
    @Test
    public void testLogin() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(LOGIN_ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", Map.of("org", "123")))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
    }
}
