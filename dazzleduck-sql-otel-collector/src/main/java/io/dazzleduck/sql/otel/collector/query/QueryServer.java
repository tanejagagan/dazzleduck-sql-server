package io.dazzleduck.sql.otel.collector.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dazzleduck.sql.common.ContentTypes;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.io.ResultStreams;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A local SQL endpoint on the collector's own DuckDB instance, for testing: {@code GET
 * /v1/query?q=<sql>} or {@code POST /v1/query} with {@code {"query": "..."}}, answering in the same
 * formats as the main server's {@code /v1/query}, chosen by {@code Accept}: TSV
 * ({@code text/tab-separated-values}), JSON Lines ({@code application/jsonl} or
 * {@code application/x-ndjson}), otherwise an Arrow IPC stream (ZSTD unless
 * {@code x-dd-arrow-compression: none}). Results stream as DuckDB produces them.
 *
 * <p>Each query runs on its own {@link ConnectionPool} connection inside {@code BEGIN TRANSACTION
 * READ ONLY}, so it sees the catalogs ingestion writes to and cannot write to their tables. That is
 * a guard against mistakes, not a security boundary: {@code COPY ... TO}, {@code SET},
 * {@code ATTACH} and DuckLake maintenance functions are not blocked. There is no authentication,
 * which is why it binds to localhost by default.
 */
public class QueryServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueryServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private enum Format { ARROW, TSV, JSONL }

    private final QuerySettings settings;
    private final MeterRegistry registry;
    private final HttpServer server;
    private final ExecutorService requests;
    private final ScheduledExecutorService timeouts;
    private final BufferAllocator allocator = new RootAllocator();

    public QueryServer(QuerySettings settings, MeterRegistry registry) throws IOException {
        this.settings = settings;
        this.registry = registry;
        this.server = HttpServer.create(new InetSocketAddress(settings.host(), settings.port()), 0);
        this.requests = Executors.newFixedThreadPool(settings.threads(), daemon("otel-query"));
        this.timeouts = Executors.newSingleThreadScheduledExecutor(daemon("otel-query-timeout"));
        server.createContext("/v1/query", this::handle);
        server.setExecutor(requests);
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    public void start() {
        server.start();
        log.info("Query endpoint listening on http://{}:{}/v1/query (read-only, no authentication)",
                settings.host(), getPort());
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Timer.Sample sample = Timer.start(registry);
        String format = "none";
        String outcome = "error";
        try {
            String sql = readQuery(exchange);
            if (sql == null) {
                outcome = "bad_request";
                return;
            }
            Format chosen = format(exchange);
            format = chosen.name().toLowerCase();
            CompressionUtil.CodecType codec = CompressionUtil.CodecType.NO_COMPRESSION;
            if (chosen == Format.ARROW) {
                codec = compression(exchange);
                if (codec == null) {
                    outcome = "bad_request";
                    return;
                }
            }
            outcome = run(exchange, sql, chosen, codec);
        } catch (Exception e) {
            log.error("Query request failed", e);
            sendText(exchange, 500, "Internal error: " + e.getMessage());
        } finally {
            exchange.close();
            sample.stop(Timer.builder("dazzleduck.otel.query.duration")
                    .description("Local query endpoint requests")
                    .tag("format", format)
                    .tag("outcome", outcome)
                    .register(registry));
        }
    }

    /** The SQL from {@code ?q=} (GET) or {@code {"query": ...}} (POST); null after sending an error. */
    private String readQuery(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String sql;
        if ("GET".equalsIgnoreCase(method)) {
            sql = queryParameter(exchange.getRequestURI().getRawQuery(), "q");
        } else if ("POST".equalsIgnoreCase(method)) {
            JsonNode body;
            try {
                body = MAPPER.readTree(exchange.getRequestBody());
            } catch (IOException e) {
                sendText(exchange, 400, "Invalid JSON body: " + e.getMessage());
                return null;
            }
            sql = body != null && body.hasNonNull("query") ? body.get("query").asText() : null;
        } else {
            exchange.getResponseHeaders().set("Allow", "GET, POST");
            sendText(exchange, 405, "Use GET ?q=<sql> or POST {\"query\": \"<sql>\"}");
            return null;
        }
        if (sql == null || sql.isBlank()) {
            sendText(exchange, 400, "Missing required parameter: q");
            return null;
        }
        return sql;
    }

    private static String queryParameter(String rawQuery, String name) {
        if (rawQuery == null) {
            return null;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            if (key.equals(name)) {
                return eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static Format format(HttpExchange exchange) {
        String accept = String.join(",", exchange.getRequestHeaders().getOrDefault("Accept", java.util.List.of()));
        if (accept.contains(ContentTypes.TEXT_TSV)) {
            return Format.TSV;
        }
        if (accept.contains(ContentTypes.APPLICATION_JSONL) || accept.contains(ContentTypes.APPLICATION_X_NDJSON)) {
            return Format.JSONL;
        }
        return Format.ARROW;
    }

    /** Same values as the main server's {@code /v1/query}; null after sending an error. */
    private static CompressionUtil.CodecType compression(HttpExchange exchange) throws IOException {
        String value = exchange.getRequestHeaders().getFirst(Headers.HEADER_ARROW_COMPRESSION);
        if (value == null) {
            return CompressionUtil.CodecType.ZSTD;
        }
        return switch (value.trim().toUpperCase()) {
            case "ZSTD", "ZSTANDARD" -> CompressionUtil.CodecType.ZSTD;
            case "NONE" -> CompressionUtil.CodecType.NO_COMPRESSION;
            default -> {
                sendText(exchange, 400, "Invalid Arrow compression codec: " + value + ". Supported values: zstd, zstandard, none");
                yield null;
            }
        };
    }

    /** Runs the query and streams the result; returns the outcome tag. */
    private String run(HttpExchange exchange, String sql, Format format,
                       CompressionUtil.CodecType codec) throws IOException {
        AtomicBoolean timedOut = new AtomicBoolean();
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement();
             BufferAllocator requestAllocator = allocator.newChildAllocator("query", 0, Long.MAX_VALUE)) {
            statement.execute("BEGIN TRANSACTION READ ONLY");
            var timeout = timeouts.schedule(() -> {
                timedOut.set(true);
                try {
                    statement.cancel();
                } catch (SQLException e) {
                    log.warn("Could not cancel a timed-out query", e);
                }
            }, settings.timeout().toMillis(), TimeUnit.MILLISECONDS);
            try {
                boolean hasResult;
                try {
                    hasResult = statement.execute(sql);
                } catch (SQLException e) {
                    if (timedOut.get()) {
                        sendText(exchange, 504, "Query timed out after " + settings.timeout());
                        return "timeout";
                    }
                    sendText(exchange, 500, e.getMessage());
                    return "error";
                }
                if (!hasResult) {
                    exchange.sendResponseHeaders(200, -1);
                    return "ok";
                }
                try (var resultSet = (DuckDBResultSet) statement.getResultSet();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(requestAllocator, settings.arrowBatchSize())) {
                    exchange.getResponseHeaders().set("Content-Type", switch (format) {
                        case TSV -> ContentTypes.TEXT_TSV_UTF8;
                        case JSONL -> ContentTypes.APPLICATION_JSONL_UTF8;
                        case ARROW -> ContentTypes.APPLICATION_ARROW;
                    });
                    exchange.sendResponseHeaders(200, 0); // chunked: rows stream as they are produced
                    try (OutputStream out = exchange.getResponseBody()) {
                        switch (format) {
                            case TSV -> ResultStreams.writeTsv(reader, out);
                            case JSONL -> ResultStreams.writeJsonl(reader, out);
                            case ARROW -> ResultStreams.writeArrow(reader, out, codec, CommonsCompressionFactory.INSTANCE);
                        }
                    } catch (IOException | RuntimeException e) {
                        // Headers are already sent, so the status cannot change; the client sees a cut-off body.
                        log.warn("Query result stream ended early{}", timedOut.get() ? " (timed out)" : "", e);
                        return timedOut.get() ? "timeout" : "error";
                    }
                }
                return "ok";
            } finally {
                timeout.cancel(false);
            }
        } catch (SQLException e) {
            sendText(exchange, 500, e.getMessage());
            return "error";
        }
    }

    private static void sendText(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = (message == null ? "" : message).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    @Override
    public void close() {
        server.stop(0);
        requests.shutdownNow();
        timeouts.shutdownNow();
        try {
            requests.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            allocator.close();
        } catch (RuntimeException e) {
            log.warn("Query allocator closed with memory still in use", e);
        }
    }
}
