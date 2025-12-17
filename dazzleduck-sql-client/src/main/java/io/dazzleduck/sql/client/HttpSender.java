package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public final class HttpSender extends FlightSender.AbstractFlightSender  {

    private static final Logger logger = LoggerFactory.getLogger(HttpSender.class);
    private final HttpClient client = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final String baseUrl;
    private final String username;
    private final String password;
    private final String targetPath;
    private final Duration timeout;

    private final long maxMem;
    private final long maxDisk;

    private volatile String jwt = null;
    private volatile Instant jwtExpiry = Instant.EPOCH;
    private static final Duration REFRESH_SKEW = Duration.ofSeconds(60);
    private static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofHours(5);
    private static final int MAX_AUTH_RETRIES = 2;

    public HttpSender(
            Schema schema,
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration timeout,
            long minBatchSize,
            Duration maxSendInterval,
            long maxInMemorySize,
            long maxOnDiskSize
    ) {
        this(schema, baseUrl, username, password, targetPath, timeout, minBatchSize, maxSendInterval, maxInMemorySize, maxOnDiskSize, Clock.systemUTC());
    }
    public HttpSender(
            Schema schema,
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration timeout,
            long minBatchSize,
            Duration maxSendInterval,
            long maxInMemorySize,
            long maxOnDiskSize,
            Clock clock
    ) {
        super(minBatchSize, maxSendInterval, schema, clock);
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

    private void login() throws Exception {
        logger.debug("Attempting login to {}", baseUrl);
        var body = mapper.writeValueAsBytes(new LoginRequest(username, password));

        var req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/login"))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();

        var resp = client.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            logger.error("Login failed with status {}", resp.statusCode());
            throw new RuntimeException("Login failed: " + resp.body());
        }

        JsonNode json = mapper.readTree(resp.body());
        LoginResponse lr = mapper.treeToValue(json, LoginResponse.class);

        this.jwt = lr.tokenType() + " " + lr.accessToken();

        long expiresIn = json.hasNonNull("expiresIn")
                ? json.get("expiresIn").asLong()
                : DEFAULT_TOKEN_LIFETIME.toSeconds();

        this.jwtExpiry = clock.instant().plusSeconds(expiresIn);
        logger.info("Login successful, JWT expires in {} seconds", expiresIn);
    }

    private String getJwt() throws Exception {
        if (jwt == null || clock.instant().isAfter(jwtExpiry.minus(REFRESH_SKEW))) {
            synchronized (this) {
                if (jwt == null || clock.instant().isAfter(jwtExpiry.minus(REFRESH_SKEW))) {
                    logger.debug("JWT refresh needed");
                    login();
                }
            }
        }
        return jwt;
    }

    @Override
    protected void doSend(SendElement element) throws InterruptedException {
        try {
            final byte[] payload;
            try (InputStream in = element.read()) {
                payload = in.readAllBytes();
            }

            HttpResponse<String> resp = null;
            int authRetries = 0;

            while (authRetries <= MAX_AUTH_RETRIES) {
                resp = post(payload);

                if (resp.statusCode() == 401 || resp.statusCode() == 403) {
                    if (authRetries >= MAX_AUTH_RETRIES) {
                        logger.error("Max auth retries ({}) exceeded for {}{}", MAX_AUTH_RETRIES, baseUrl, targetPath);
                        break;
                    }
                    logger.warn("Received auth failure ({}) on attempt {}, invalidating JWT and retrying",
                            resp.statusCode(), authRetries + 1);
                    synchronized (this) {
                        jwt = null;
                    }
                    authRetries++;
                } else {
                    // Non-auth response, break the retry loop
                    break;
                }
            }

            if (resp.statusCode() != 200) {
                logger.error("Ingestion failed with status {} to {}{}", resp.statusCode(), baseUrl, targetPath);
                throw new RuntimeException("Ingestion failed: " + resp.body());
            }

            logger.debug("Successfully sent data to {}{}", baseUrl, targetPath);

        } catch (InterruptedException e) {
            // Re-throw to allow graceful shutdown
            throw e;
        } catch (Exception e) {
            logger.error("Failed to send data to {}{}", baseUrl, targetPath, e);
            throw new RuntimeException("Failed to send data to " + baseUrl + targetPath, e);
        }
    }

    private HttpResponse<String> post(byte[] payload) throws Exception {
        var req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/ingest?path=" + targetPath))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .header("Authorization", getJwt())
                .header("Content-Type", "application/vnd.apache.arrow.stream")
                .build();

        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}