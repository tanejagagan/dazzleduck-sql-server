package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

public final class HttpSender extends FlightSender.AbstractFlightSender {

    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final String username;
    private final String password;
    private final String targetPath; // file path like "abc.parquet"
    private final Duration timeout;

    private final long maxMem;
    private final long maxDisk;

    private String jwt = null;
    private Instant jwtExpiry = Instant.EPOCH;

    public HttpSender(
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration timeout,
            long maxInMemorySize,
            long maxOnDiskSize
    ) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.targetPath = targetPath;
        this.timeout = timeout;
        this.maxMem = maxInMemorySize;
        this.maxDisk = maxOnDiskSize;
    }

    @Override
    public long getMaxInMemorySize() {
        return maxMem;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxDisk;
    }

    private synchronized void login() throws Exception {
        var loginUrl = baseUrl + "/login";
        var body = mapper.writeValueAsBytes(new LoginRequest(username, password));

        var req = HttpRequest.newBuilder()
                .uri(URI.create(loginUrl))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Accept", "application/json")
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            throw new RuntimeException("Login failed: " + resp.body());
        }

        LoginResponse lr = mapper.readValue(resp.body(), LoginResponse.class);
        this.jwt = lr.tokenType() + " " + lr.accessToken();

        // JWT expiry handling (short lifetime for tests)
        this.jwtExpiry = Instant.now().plusSeconds(1);
    }

    private synchronized String getJwt() throws Exception {
        if (jwt == null || Instant.now().isAfter(jwtExpiry)) {
            login();
        }
        return jwt;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        try {
            InputStream is = element.read();
            String url = baseUrl + "/ingest?path=" + targetPath;

            var req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(timeout)
                    .POST(HttpRequest.BodyPublishers.ofInputStream(() -> is))
                    .header("Authorization", getJwt())
                    .header("Content-Type", "application/vnd.apache.arrow.stream")
                    .build();

            var resp = client.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() != 200) {
                throw new RuntimeException("Ingestion failed: " + resp.body());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
