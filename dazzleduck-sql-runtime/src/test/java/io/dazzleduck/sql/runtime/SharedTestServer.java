package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Test server that provides a Runtime instance for integration tests.
 * Each test class can have its own instance with isolated ports and warehouse.
 *
 * <p>Usage:
 * <pre>
 * public class MyIntegrationTest {
 *     private SharedTestServer server;
 *
 *     @BeforeAll
 *     void setup() throws Exception {
 *         server = new SharedTestServer();
 *         server.start();
 *     }
 *
 *     @AfterAll
 *     void teardown() {
 *         server.close();
 *     }
 *
 *     @Test
 *     void myTest() {
 *         String baseUrl = server.getHttpBaseUrl();
 *         // ... test code
 *     }
 * }
 * </pre>
 */
public final class SharedTestServer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SharedTestServer.class);

    private Runtime runtime;
    private Config config;
    private int httpPort;
    private int flightPort;
    private String warehousePath;
    private boolean started = false;

    public SharedTestServer() {
    }

    /**
     * Starts the test server with default configuration and dynamic ports.
     */
    public void start() throws Exception {
        start(new String[0]);
    }

    /**
     * Starts the test server with custom config overrides and dynamic ports.
     */
    public void start(String... configOverrides) throws Exception {
        startWithPorts(0, 0, configOverrides);
    }

    /**
     * Starts the test server with fixed ports.
     * Use this when tests depend on config files with hardcoded ports (e.g., ArrowSimpleLogger).
     *
     * @param fixedHttpPort the HTTP port to use (0 for dynamic)
     * @param fixedFlightPort the Flight SQL port to use (0 for dynamic)
     * @param configOverrides additional config overrides
     */
    public void startWithPorts(int fixedHttpPort, int fixedFlightPort, String... configOverrides) throws Exception {
        if (started) {
            return;
        }

        httpPort = fixedHttpPort > 0 ? fixedHttpPort : findAvailablePort();
        flightPort = fixedFlightPort > 0 ? fixedFlightPort : findAvailablePort();
        warehousePath = Files.createTempDirectory("test-warehouse").toString();

        logger.info("Starting test server - HTTP port: {}, Flight port: {}, Warehouse: {}",
                httpPort, flightPort, warehousePath);

        Config baseConfig = ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH);

        config = baseConfig
                .withValue("networking_modes", ConfigValueFactory.fromIterable(List.of("http", "flight-sql")))
                .withValue("http.port", ConfigValueFactory.fromAnyRef(httpPort))
                .withValue("flight_sql.port", ConfigValueFactory.fromAnyRef(flightPort))
                .withValue("flight_sql.host", ConfigValueFactory.fromAnyRef("localhost"))
                .withValue("flight_sql.use_encryption", ConfigValueFactory.fromAnyRef(false))
                .withValue("warehouse", ConfigValueFactory.fromAnyRef(warehousePath))
                .withValue("http.auth", ConfigValueFactory.fromAnyRef("jwt"))
                .withValue("ingestion.max_delay_ms", ConfigValueFactory.fromAnyRef(50));

        for (String override : configOverrides) {
            String[] parts = override.split("=", 2);
            if (parts.length == 2) {
                config = config.withValue(parts[0].trim(),
                        ConfigValueFactory.fromAnyRef(parseValue(parts[1].trim())));
            }
        }

        runtime = Runtime.start(config);

        waitForServer("localhost", httpPort, 10_000);
        waitForServer("localhost", flightPort, 10_000);

        started = true;
        logger.info("Test server started successfully");
    }

    @Override
    public void close() {
        if (!started) {
            return;
        }

        logger.info("Shutting down test server");

        if (runtime != null) {
            try {
                runtime.close();
            } catch (Exception e) {
                logger.error("Error closing runtime", e);
            }
            runtime = null;
        }

        if (warehousePath != null) {
            try {
                Path path = Path.of(warehousePath);
                if (Files.exists(path)) {
                    Files.walk(path)
                            .sorted(Comparator.reverseOrder())
                            .forEach(p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException ignored) {
                                }
                            });
                }
            } catch (IOException e) {
                logger.warn("Failed to cleanup warehouse directory: {}", warehousePath, e);
            }
        }

        started = false;
    }

    /**
     * @return The HTTP port the server is listening on
     */
    public int getHttpPort() {
        ensureStarted();
        return httpPort;
    }

    /**
     * @return The Flight SQL port the server is listening on
     */
    public int getFlightPort() {
        ensureStarted();
        return flightPort;
    }

    /**
     * @return The base URL for HTTP requests (e.g., "http://localhost:8093/")
     */
    public String getHttpBaseUrl() {
        ensureStarted();
        return "http://localhost:" + httpPort + "/";
    }

    /**
     * @return The warehouse path used by the server
     */
    public String getWarehousePath() {
        ensureStarted();
        return warehousePath;
    }

    /**
     * @return The config used to start the server
     */
    public Config getConfig() {
        ensureStarted();
        return config;
    }

    /**
     * @return The Runtime instance (for advanced use cases)
     */
    public Runtime getRuntime() {
        ensureStarted();
        return runtime;
    }

    /**
     * Creates a subdirectory in the warehouse for test isolation.
     *
     * @param name The subdirectory name
     * @return The path to the created subdirectory
     */
    public Path createTestDirectory(String name) throws IOException {
        ensureStarted();
        Path dir = Path.of(warehousePath, name);
        Files.createDirectories(dir);
        return dir;
    }

    /**
     * Generates a unique test path based on prefix and timestamp.
     *
     * @param prefix A prefix for the path
     * @return A unique path string
     */
    public String uniqueTestPath(String prefix) {
        return prefix + "-" + System.nanoTime();
    }

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("Server not started. Call start() first.");
        }
    }

    private static Object parseValue(String value) {
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private static int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void waitForServer(String host, int port, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket(host, port)) {
                logger.debug("Server is ready on {}:{}", host, port);
                return;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }

        throw new RuntimeException("Server did not start within " + timeoutMs + "ms on " + host + ":" + port);
    }
}
