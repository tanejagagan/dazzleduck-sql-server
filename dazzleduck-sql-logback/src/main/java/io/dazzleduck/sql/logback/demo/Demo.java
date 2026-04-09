package io.dazzleduck.sql.logback.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Continuous demo for the Logback LogForwardingAppender.
 *
 * <p>This demo generates sample logs continuously (every 500ms) that are forwarded
 * to a DazzleDuck server via the LogForwardingAppender configured in logback.xml.
 * Press Ctrl+C to stop.
 *
 * <h2>Prerequisites</h2>
 * <ol>
 *   <li>Start a DazzleDuck server on localhost:8081</li>
 *   <li>Configure logback.xml with LogForwardingAppender</li>
 * </ol>
 *
 * <h2>Run</h2>
 * <pre>
 * ./mvnw exec:java -pl dazzleduck-sql-logback \
 *   -Dexec.mainClass="io.dazzleduck.sql.logback.demo.Demo"
 * </pre>
 */
public class Demo {

    private static final Logger log = LoggerFactory.getLogger(Demo.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong counter = new AtomicLong(0);
    private static final Random random = new Random();
    private static final ScheduledExecutorService queryScheduler = Executors.newSingleThreadScheduledExecutor();

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicReference<String> cachedJwt = new AtomicReference<>();

    private static final String[] USERS = {"alice", "bob", "charlie", "diana", "eve"};
    private static final String[] ACTIONS = {"LOGIN", "LOGOUT", "VIEW", "UPDATE", "DELETE"};
    private static final String[] ENDPOINTS = {"/api/users", "/api/orders", "/api/products"};

    // OpenTelemetry standard MDC fields
    private static String hostName;
    private static String applicationId;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            queryScheduler.shutdown();
            System.out.println("Demo stopped. Total logs generated: " + counter.get());
        }));

        String baseUrl = System.getenv().getOrDefault("DAZZLEDUCK_BASE_URL", "http://localhost:8081");
        try {
            hostName = System.getenv().getOrDefault("HOSTNAME", java.net.InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            hostName = "unknown-host";
        }
        applicationId = System.getenv().getOrDefault("APPLICATION_ID", "logback-demo");
        System.out.println("Demo started - forwarding logs to " + baseUrl);

        // Report ingested log count every 30 seconds
        queryScheduler.scheduleAtFixedRate(() -> runCountQuery(baseUrl), 30, 30, TimeUnit.SECONDS);

        while (running.get()) {
            try {
                generateLog();
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static String login(String baseUrl) throws Exception {
        String username = System.getenv().getOrDefault("DAZZLEDUCK_USERNAME", "admin");
        String password = System.getenv().getOrDefault("DAZZLEDUCK_PASSWORD", "admin");
        java.util.Map<String, String> credentials = new java.util.HashMap<>();
        credentials.put("username", username);
        credentials.put("password", password);
        String body = MAPPER.writeValueAsString(credentials);

        URL url = new URL(baseUrl + "/v1/login");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        if (conn.getResponseCode() != 200) {
            throw new RuntimeException("Login failed: HTTP " + conn.getResponseCode());
        }

        try (InputStream is = conn.getInputStream()) {
            JsonNode json = MAPPER.readTree(is);
            return json.get("tokenType").asText() + " " + json.get("accessToken").asText();
        } finally {
            conn.disconnect();
        }
    }

    private static String getJwt(String baseUrl) {
        String jwt = cachedJwt.get();
        if (jwt == null) {
            try {
                jwt = login(baseUrl);
                cachedJwt.set(jwt);
            } catch (Exception e) {
                System.err.println("Login failed: " + e.getMessage());
            }
        }
        return jwt;
    }

    private static void runCountQuery(String baseUrl) {
        try (BufferAllocator allocator = new RootAllocator()) {
            String jwt = getJwt(baseUrl);
            if (jwt == null) {
                System.err.println("Skipping count query: no JWT token available");
                return;
            }

            String query = "SELECT count(*) as count FROM ollylake.main.log";
            String urlStr = baseUrl + "/v1/query?q=" + java.net.URLEncoder.encode(query, "UTF-8");
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", jwt);

            int status = conn.getResponseCode();
            if (status == 401 || status == 403) {
                // Token may have expired — invalidate and retry once
                cachedJwt.set(null);
                jwt = getJwt(baseUrl);
                if (jwt == null) return;
                conn.disconnect();
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setRequestProperty("Authorization", jwt);
                status = conn.getResponseCode();
            }

            if (status == 200) {
                try (InputStream inputStream = new BufferedInputStream(conn.getInputStream());
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {

                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        if (root.getRowCount() > 0) {
                            org.apache.arrow.vector.BigIntVector countVector = (org.apache.arrow.vector.BigIntVector) root.getVector("count");
                            long count = countVector.getObject(0);
                            System.out.println("Ingested log count: " + count);
                        }
                    }
                }
            } else {
                System.err.println("Count query failed: HTTP " + status);
            }
            conn.disconnect();
        } catch (Exception e) {
            System.err.println("Error running count query: " + e.getMessage());
        }
    }

    private static void generateLog() {
        long count = counter.incrementAndGet();
        String requestId = "REQ-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String user = USERS[random.nextInt(USERS.length)];

        // OpenTelemetry standard MDC fields
        MDC.put("host.name", hostName);
        MDC.put("application.id", applicationId);
        MDC.put("request_id", requestId);
        MDC.put("user_id", user);
        MDC.put("log_count", String.valueOf(count));

        try {
            int scenario = random.nextInt(100);

            if (scenario < 50) {
                // Normal info log
                String action = ACTIONS[random.nextInt(ACTIONS.length)];
                log.info("User {} performed action: {}", user, action);
            } else if (scenario < 70) {
                // API request log
                String endpoint = ENDPOINTS[random.nextInt(ENDPOINTS.length)];
                int responseTime = random.nextInt(500) + 10;
                log.info("API request to {} completed in {}ms", endpoint, responseTime);
            } else if (scenario < 85) {
                // Warning log
                log.warn("High latency detected for user {}: {}ms", user, random.nextInt(1000) + 500);
            } else if (scenario < 95) {
                // Debug log
                log.debug("Processing batch item {} for user {}", count, user);
            } else {
                // Error log with exception
                try {
                    throw new RuntimeException("Simulated error #" + count);
                } catch (Exception e) {
                    log.error("Error processing request for user {}", user, e);
                }
            }
        } finally {
            MDC.clear();
        }
    }
}
