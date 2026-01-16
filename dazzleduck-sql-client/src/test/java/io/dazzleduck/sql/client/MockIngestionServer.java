package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mock HTTP server that mimics the DazzleDuck HTTP ingestion service.
 *
 * This server can be used for testing the HttpArrowProducer without needing the full
 * DazzleDuck server stack. It provides:
 *
 * - POST /v1/login - Returns a mock JWT token
 * - POST /v1/ingest?path=<path> - Stores Arrow stream data to the warehouse directory
 *
 * Usage:
 * <pre>
 * // Programmatic usage in tests
 * MockIngestionServer server = new MockIngestionServer(8080, "/path/to/warehouse");
 * server.start();
 * // ... run tests ...
 * server.stop();
 *
 * // Command-line usage (separate process)
 * java -cp <classpath> io.dazzleduck.sql.client.MockIngestionServer 8080 /path/to/warehouse
 * </pre>
 */
public class MockIngestionServer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MockIngestionServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String CONTENT_TYPE_ARROW = "application/vnd.apache.arrow.stream";

    private volatile HttpServer server;
    private final Path warehousePath;
    private final int port;
    private final String username;
    private final String password;
    private final AtomicLong fileCounter = new AtomicLong(0);

    // Track ingested data for verification in tests
    private final AtomicLong totalBytesIngested = new AtomicLong(0);
    private final AtomicLong totalFilesWritten = new AtomicLong(0);

    // Store valid JWT tokens
    private final Set<String> validTokens = ConcurrentHashMap.newKeySet();

    /**
     * Creates a mock ingestion server with default credentials (admin/admin).
     *
     * @param port          The port to listen on
     * @param warehousePath The directory where ingested data will be stored
     */
    public MockIngestionServer(int port, Path warehousePath) throws IOException {
        this(port, warehousePath, "admin", "admin");
    }

    /**
     * Creates a mock ingestion server with custom credentials.
     *
     * @param port          The port to listen on
     * @param warehousePath The directory where ingested data will be stored
     * @param username      The username for authentication
     * @param password      The password for authentication
     */
    public MockIngestionServer(int port, Path warehousePath, String username, String password) throws IOException {
        this.port = port;
        this.warehousePath = warehousePath;
        this.username = username;
        this.password = password;

        // Create warehouse directory if it doesn't exist
        Files.createDirectories(warehousePath);

        logger.info("MockIngestionServer configured on port {} with warehouse at {}", port, warehousePath);
    }

    /**
     * Creates a new HttpServer instance configured with handlers.
     */
    private HttpServer createServer() throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(4));
        httpServer.createContext("/v1/login", new LoginHandler());
        httpServer.createContext("/v1/ingest", new IngestHandler());
        return httpServer;
    }

    /**
     * Starts the server. Can be called after stop() to restart.
     */
    public void start() {
        try {
            this.server = createServer();
            server.start();
            logger.info("MockIngestionServer started on port {}", port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start MockIngestionServer", e);
        }
    }

    /**
     * Stops the server. Can be restarted by calling start() again.
     */
    public void stop() {
        if (server != null) {
            server.stop(1);
            server = null;
            logger.info("MockIngestionServer stopped");
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Returns the port the server is listening on.
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the base URL of the server.
     */
    public String getBaseUrl() {
        return "http://localhost:" + port;
    }

    /**
     * Returns the total bytes ingested since server start.
     */
    public long getTotalBytesIngested() {
        return totalBytesIngested.get();
    }

    /**
     * Returns the total files written since server start.
     */
    public long getTotalFilesWritten() {
        return totalFilesWritten.get();
    }

    /**
     * Resets the ingestion statistics.
     */
    public void resetStats() {
        totalBytesIngested.set(0);
        totalFilesWritten.set(0);
    }

    /**
     * Handler for POST /v1/login
     * Accepts username/password and returns a mock JWT token.
     */
    private class LoginHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            try {
                // Parse login request
                byte[] requestBody = exchange.getRequestBody().readAllBytes();
                Map<String, Object> loginRequest = mapper.readValue(requestBody, Map.class);

                String reqUsername = (String) loginRequest.get("username");
                String reqPassword = (String) loginRequest.get("password");

                // Validate credentials
                if (!username.equals(reqUsername) || !password.equals(reqPassword)) {
                    sendError(exchange, 401, "Invalid credentials");
                    return;
                }

                // Generate mock JWT token and store it
                String token = generateMockToken(reqUsername);
                validTokens.add(token);

                // Generate mock JWT response (must match LoginResponse record fields)
                Map<String, Object> response = new HashMap<>();
                response.put("accessToken", token);
                response.put("username", reqUsername);
                response.put("tokenType", "Bearer");

                byte[] responseBody = mapper.writeValueAsBytes(response);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, responseBody.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBody);
                }

                logger.debug("Login successful for user: {}", reqUsername);

            } catch (Exception e) {
                logger.error("Login failed", e);
                sendError(exchange, 500, "Login failed: " + e.getMessage());
            }
        }

        private String generateMockToken(String username) {
            // Generate a simple mock token (not a real JWT, but sufficient for testing)
            return "mock-token-" + username + "-" + System.currentTimeMillis();
        }
    }

    /**
     * Handler for POST /v1/ingest?path=<path>
     * Accepts Arrow stream data and stores it to the warehouse.
     */
    private class IngestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            try {
                // Validate Content-Type
                String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                if (contentType == null || !contentType.contains(CONTENT_TYPE_ARROW)) {
                    sendError(exchange, 415, "Unsupported Media Type. Expected: " + CONTENT_TYPE_ARROW);
                    return;
                }

                // Extract path parameter
                String query = exchange.getRequestURI().getQuery();
                String path = extractPathParameter(query);
                if (path == null || path.isEmpty()) {
                    sendError(exchange, 400, "Missing required parameter: path");
                    return;
                }

                // Validate path (security check)
                if (path.contains("..") || path.startsWith("/")) {
                    sendError(exchange, 400, "Invalid path: must not contain '..' or start with '/'");
                    return;
                }

                // Validate Authorization header - token must be valid
                String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
                if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                    sendError(exchange, 401, "Missing or invalid Authorization header");
                    return;
                }

                String token = authHeader.substring("Bearer ".length()).trim();
                if (!validTokens.contains(token)) {
                    sendError(exchange, 401, "Invalid or expired token");
                    return;
                }

                // Create target directory
                Path targetDir = warehousePath.resolve(path);
                Files.createDirectories(targetDir);

                // Generate unique filename
                String filename = String.format("ingestion_%s_%d.arrow",
                        UUID.randomUUID().toString().substring(0, 8),
                        fileCounter.incrementAndGet());
                Path targetFile = targetDir.resolve(filename);

                // Read and store the Arrow data
                try (InputStream is = exchange.getRequestBody();
                     OutputStream os = Files.newOutputStream(targetFile)) {

                    long bytesWritten = is.transferTo(os);
                    totalBytesIngested.addAndGet(bytesWritten);
                    totalFilesWritten.incrementAndGet();

                    logger.debug("Ingested {} bytes to {}", bytesWritten, targetFile);
                }

                // Send success response
                exchange.sendResponseHeaders(200, 0);
                exchange.getResponseBody().close();

            } catch (Exception e) {
                logger.error("Ingestion failed", e);
                sendError(exchange, 500, "Ingestion failed: " + e.getMessage());
            }
        }

        private String extractPathParameter(String query) {
            if (query == null) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] parts = param.split("=", 2);
                if (parts.length == 2 && "path".equals(parts[0])) {
                    return URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                }
            }
            return null;
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        Map<String, String> errorResponse = Map.of("error", message);
        byte[] responseBody = mapper.writeValueAsBytes(errorResponse);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBody);
        }
    }

    /**
     * Main method for running the server as a standalone process.
     *
     * Usage: java io.dazzleduck.sql.client.MockIngestionServer <port> <warehouse_path> [username] [password]
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java io.dazzleduck.sql.client.MockIngestionServer <port> <warehouse_path> [username] [password]");
            System.err.println("Example: java io.dazzleduck.sql.client.MockIngestionServer 8080 /tmp/warehouse");
            System.err.println("Example: java io.dazzleduck.sql.client.MockIngestionServer 8080 /tmp/warehouse myuser mypass");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        Path warehousePath = Path.of(args[1]);
        String username = args.length > 2 ? args[2] : "admin";
        String password = args.length > 3 ? args[3] : "admin";

        try {
            MockIngestionServer server = new MockIngestionServer(port, warehousePath, username, password);
            server.start();

            System.out.println("MockIngestionServer running on http://localhost:" + port);
            System.out.println("Warehouse directory: " + warehousePath.toAbsolutePath());
            System.out.println("Credentials: " + username + "/" + password);
            System.out.println("Press Ctrl+C to stop...");

            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down...");
                server.stop();
            }));

            // Keep main thread alive
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
