package io.dazzleduck.sql.otel.collector.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dazzleduck.sql.commons.ingestion.Stats;
import io.dazzleduck.sql.commons.ingestion.StatsHtml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

/**
 * Embedded HTTP health endpoint for the otel-collector. {@code GET /health} reports the current
 * {@link CollectorHealthStatus}: {@code 200} for {@code HEALTHY}, {@code 503} for
 * {@code MAINTENANCE}/{@code DOWN} — so a readiness probe or load balancer stops routing traffic
 * the instant the collector leaves {@code HEALTHY}, ahead of the gRPC server actually stopping.
 *
 * <p>{@code GET /stats} additionally serves an auto-refreshing HTML dashboard of per-queue ingestion
 * stats (rendered by {@link StatsHtml}, the same renderer the main server's {@code /v1/ui} uses),
 * sourced from the supplied {@code statsSupplier}.
 */
public class HealthServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HealthServer.class);

    private final HttpServer server;

    public HealthServer(int port, CollectorHealth health, int grpcPort,
                        Supplier<List<Stats>> statsSupplier) throws IOException {
        this(port, health, grpcPort, statsSupplier, null);
    }

    /**
     * @param extraSection HTML appended to {@code /stats} below the queue table (e.g. compaction
     *                     status), or null for none
     */
    public HealthServer(int port, CollectorHealth health, int grpcPort,
                        Supplier<List<Stats>> statsSupplier, Supplier<String> extraSection) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> handle(exchange, health, grpcPort));
        server.createContext("/stats", exchange -> handleStats(exchange, statsSupplier, extraSection));
        server.setExecutor(null);
    }

    public void start() {
        server.start();
        logger.info("Health server listening on port {}", server.getAddress().getPort());
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange, CollectorHealth health, int grpcPort) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        CollectorHealthStatus status = health.getStatus();
        int statusCode = status == CollectorHealthStatus.HEALTHY ? 200 : 503;
        byte[] body = toJson(status, health, grpcPort).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void handleStats(HttpExchange exchange, Supplier<List<Stats>> statsSupplier,
                             Supplier<String> extraSection) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        List<Stats> stats;
        try {
            stats = statsSupplier.get();
        } catch (RuntimeException e) {
            logger.warn("Failed to collect queue stats for /stats", e);
            stats = List.of();
        }
        String page = StatsHtml.renderPage(stats, "DazzleDuck OTLP Collector — Queue Stats", 5);
        if (extraSection != null) {
            String section;
            try {
                section = extraSection.get();
            } catch (RuntimeException e) {
                logger.warn("Failed to render an extra /stats section", e);
                section = "";
            }
            int end = page.lastIndexOf("</body>");
            page = page.substring(0, end) + section + page.substring(end);
        }
        byte[] body = page.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private String toJson(CollectorHealthStatus status, CollectorHealth health, int grpcPort) {
        return "{\n"
                + "  \"status\": \"" + status + "\",\n"
                + "  \"uptimeSeconds\": " + health.uptimeSeconds() + ",\n"
                + "  \"grpcPort\": " + grpcPort + ",\n"
                + "  \"knownQueues\": " + health.knownQueueCount() + ",\n"
                + "  \"batchesProcessed\": " + health.batchesProcessed() + "\n"
                + "}";
    }
}
