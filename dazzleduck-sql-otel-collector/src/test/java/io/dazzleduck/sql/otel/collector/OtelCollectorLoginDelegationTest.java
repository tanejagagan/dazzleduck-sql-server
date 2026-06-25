package io.dazzleduck.sql.otel.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for OtelCollectorServer JWT authentication,
 * covering both login delegation (login_url configured) and local auth.
 */
public class OtelCollectorLoginDelegationTest {

    @org.junit.jupiter.api.BeforeAll
    static void loadExtensions() throws Exception {
        // Arrow must be loaded before OtelCollectorServer starts — the server no longer
        // runs the startup script internally; Main handles it in production.
        io.dazzleduck.sql.commons.ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community", "LOAD arrow"
        });
    }

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    static final String VALID_USER = "admin";
    static final String VALID_PASS = "admin";
    static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    static final ObjectMapper MAPPER = new ObjectMapper();

    static int findFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    static Metadata basicAuthMetadata(String username, String password) {
        String encoded = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Basic " + encoded);
        return meta;
    }

    static ExportLogsServiceRequest sampleRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder().build())
                                .build())
                        .build())
                .build();
    }

    static String generateValidToken() {
        return generateTokenWithQueueClaim("logs");
    }

    static String generateTokenWithQueueClaim(String queueId) {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        return Jwts.builder()
                .subject(VALID_USER)
                .claim(Headers.CLAIM_INGESTION_QUEUE, queueId)
                .expiration(exp.getTime())
                .signWith(key)
                .compact();
    }

    // -------------------------------------------------------------------------
    // Delegation tests: login_url points to stub HTTP login service
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithLoginDelegation {

        HttpServer stubLoginServer;
        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int stubPort = findFreePort();
            stubLoginServer = startStubLoginServer(stubPort);

            int otelPort = findFreePort();
            var outputPath = Files.createTempDirectory("otel-test-delegation").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(otelPort);
            props.setIngestionHandler(noopHandler(outputPath.toString()));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setLoginUrl("http://localhost:" + stubPort + "/v1/login");
            props.setJwtExpiration(Duration.ofHours(1));
            // minBucketSize=1 flushes every batch immediately, so export() returns without waiting
            // for the time-based flush (the default 5s max_delay made each method take ~5s).
            props.setIngestionConfig(ingestionConfig(1L, 60_000L));

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
            if (stubLoginServer != null) stubLoginServer.stop(0);
        }

        @Test
        void validCredentials_delegated_succeeds() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, VALID_PASS)));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidCredentials_delegated_rejected() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, "wrongpassword")));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void noAuthHeader_rejected() {
            var ex = assertThrows(StatusRuntimeException.class, () -> stub.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void validBearerToken_accepted() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidBearerToken_rejected() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer not.a.valid.token");
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        /**
         * Starts a minimal HTTP server that validates credentials and returns a signed JWT,
         * or 401 on invalid credentials. Uses the same secret key as the otel-collector.
         */
        private HttpServer startStubLoginServer(int port) throws IOException {
            SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
            var server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v1/login", exchange -> {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    exchange.close();
                    return;
                }
                try {
                    var node = MAPPER.readTree(exchange.getRequestBody().readAllBytes());
                    String username = node.get("username").asText();
                    String password = node.get("password").asText();

                    if (VALID_USER.equals(username) && VALID_PASS.equals(password)) {
                        Calendar exp = Calendar.getInstance();
                        exp.add(Calendar.HOUR, 1);
                        String token = Jwts.builder()
                                .subject(username)
                                .claim(Headers.CLAIM_INGESTION_QUEUE, "logs")
                                .expiration(exp.getTime())
                                .signWith(key)
                                .compact();
                        byte[] resp = MAPPER.writeValueAsBytes(
                                Map.of("accessToken", token, "tokenType", "Bearer"));
                        exchange.getResponseHeaders().add("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, resp.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(resp);
                        }
                    } else {
                        exchange.sendResponseHeaders(401, -1);
                    }
                } catch (Exception e) {
                    exchange.sendResponseHeaders(500, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
            return server;
        }
    }

    // -------------------------------------------------------------------------
    // Local auth tests: no login_url, users validated from config
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithLocalAuth {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            var outputPath = Files.createTempDirectory("otel-test-local").resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(otelPort);
            props.setIngestionHandler(noopHandler(outputPath.toString()));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setUsers(Map.of(VALID_USER, VALID_PASS));
            props.setJwtExpiration(Duration.ofHours(1));
            // no loginUrl → local credential validation
            // minBucketSize=1 flushes every batch immediately, so export() returns without waiting
            // for the time-based flush (the default 5s max_delay made each method take ~5s).
            props.setIngestionConfig(ingestionConfig(1L, 60_000L));

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            stub = LogsServiceGrpc.newBlockingStub(channel);
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void validCredentials_local_succeeds() {
            // Bearer token with queue claim — local auth validates the credential path end-to-end
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }

        @Test
        void invalidCredentials_local_rejected() {
            var s = stub.withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(basicAuthMetadata(VALID_USER, "wrongpassword")));
            var ex = assertThrows(StatusRuntimeException.class, () -> s.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void noAuthHeader_local_rejected() {
            var ex = assertThrows(StatusRuntimeException.class, () -> stub.export(sampleRequest()));
            assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
        }

        @Test
        void validBearerToken_local_accepted() {
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            var s = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
            assertDoesNotThrow(() -> s.export(sampleRequest()));
        }
    }

    // -------------------------------------------------------------------------
    // Transformation tests: derived columns appear in written Parquet
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithTransformations {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        LogsServiceGrpc.LogsServiceBlockingStub stub;
        Path outputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-transform");
            outputPath = tempDir.resolve("output");
            Files.createDirectories(outputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(otelPort);
            props.setIngestionHandler(noopHandler(outputPath.toString(),
                    "SELECT *, severity_number * 2 as doubled_severity FROM __this"));
            props.setIngestionConfig(ingestionConfig(1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateValidToken());
            stub = LogsServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        /**
         * Sends a log record and verifies the derived column and all original columns
         * are present in the written Parquet file.
         * The response is only sent after the Parquet write completes, so the file
         * is guaranteed to exist by the time stub.export() returns.
         */
        @Test
        void derivedAndOriginalColumns_presentInParquet() throws Exception {
            stub.export(ExportLogsServiceRequest.newBuilder()
                    .addResourceLogs(ResourceLogs.newBuilder()
                            .addScopeLogs(ScopeLogs.newBuilder()
                                    .addLogRecords(LogRecord.newBuilder()
                                            .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                            .setSeverityText("INFO")
                                            .build())
                                    .build())
                            .build())
                    .build());

            // severity_number=9 (OTLP INFO), doubled_severity = 9 * 2 = 18
            TestUtils.isEqual(
                    "SELECT 9 as severity_number, 'INFO' as severity_text, 18 as doubled_severity",
                    "SELECT severity_number, severity_text, doubled_severity FROM read_parquet('" + outputPath + "')"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Trace ingestion tests: spans land in Parquet with correct schema
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithTraces {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        TraceServiceGrpc.TraceServiceBlockingStub stub;
        Path tracesOutputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-traces");
            tracesOutputPath = tempDir.resolve("traces");
            Files.createDirectories(tracesOutputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(otelPort);
            props.setIngestionHandler(noopHandler(tracesOutputPath.toString()));
            props.setIngestionConfig(ingestionConfig(1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateTokenWithQueueClaim("traces"));
            stub = TraceServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void spanFields_presentInParquet() throws Exception {
            byte[] traceIdBytes = new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
            byte[] spanIdBytes  = new byte[]{0,1,2,3,4,5,6,7};
            long startNanos = 1_000_000_000_000L; // 1_000_000 ms
            long endNanos   = 1_001_000_000_000L; // 1_001_000 ms → duration=1000ms

            stub.export(ExportTraceServiceRequest.newBuilder()
                    .addResourceSpans(ResourceSpans.newBuilder()
                            .addScopeSpans(ScopeSpans.newBuilder()
                                    .addSpans(Span.newBuilder()
                                            .setTraceId(ByteString.copyFrom(traceIdBytes))
                                            .setSpanId(ByteString.copyFrom(spanIdBytes))
                                            .setName("test-span")
                                            .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                            .setStartTimeUnixNano(startNanos)
                                            .setEndTimeUnixNano(endNanos)
                                            .setStatus(io.opentelemetry.proto.trace.v1.Status.newBuilder()
                                                    .setCode(io.opentelemetry.proto.trace.v1.Status.StatusCode.STATUS_CODE_OK)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

            TestUtils.isEqual(
                    "SELECT 'test-span' as name, 'SERVER' as kind, 1000 as duration_ms, 'OK' as status_code",
                    "SELECT name, kind, duration_ms, status_code FROM read_parquet('" + tracesOutputPath + "')"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Metrics ingestion tests: data points land in Parquet with correct schema
    // -------------------------------------------------------------------------

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class WithMetrics {

        OtelCollectorServer otelServer;
        ManagedChannel channel;
        MetricsServiceGrpc.MetricsServiceBlockingStub stub;
        Path metricsOutputPath;

        @BeforeAll
        void setup() throws Exception {
            int otelPort = findFreePort();
            Path tempDir = Files.createTempDirectory("otel-test-metrics");
            metricsOutputPath = tempDir.resolve("metrics");
            Files.createDirectories(metricsOutputPath); // operator provisions the output dir

            CollectorProperties props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
            props.setGrpcPort(otelPort);
            props.setIngestionHandler(noopHandler(metricsOutputPath.toString()));
            props.setIngestionConfig(ingestionConfig(1L, 60_000L));
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);

            otelServer = new OtelCollectorServer(props);
            otelServer.start();

            channel = ManagedChannelBuilder.forAddress("localhost", otelPort)
                    .usePlaintext()
                    .build();
            var meta = new Metadata();
            meta.put(AUTHORIZATION_KEY, "Bearer " + generateTokenWithQueueClaim("metrics"));
            stub = MetricsServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(meta));
        }

        @AfterAll
        void cleanup() throws Exception {
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (otelServer != null) otelServer.close();
        }

        @Test
        void gaugeMetric_presentInParquet() throws Exception {
            stub.export(ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(ResourceMetrics.newBuilder()
                            .addScopeMetrics(ScopeMetrics.newBuilder()
                                    .addMetrics(Metric.newBuilder()
                                            .setName("cpu.usage")
                                            .setUnit("1")
                                            .setGauge(Gauge.newBuilder()
                                                    .addDataPoints(NumberDataPoint.newBuilder()
                                                            .setAsDouble(0.42)
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build());

            TestUtils.isEqual(
                    "SELECT 'cpu.usage' as name, 'GAUGE' as metric_type, 0.42 as value_double",
                    "SELECT name, metric_type, value_double FROM read_parquet('" + metricsOutputPath + "')"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers — replace setXIngestionConfig with handler + IngestionConfig
    // -----------------------------------------------------------------------

    /** NOOP handler that returns {@code outputPath} for every queue ID. */
    private static io.dazzleduck.sql.commons.ingestion.IngestionHandler noopHandler(String outputPath) {
        return noopHandler(outputPath, null);
    }

    private static io.dazzleduck.sql.commons.ingestion.IngestionHandler noopHandler(
            String outputPath, String transformation) {
        return new io.dazzleduck.sql.commons.ingestion.IngestionHandler() {
            @Override public io.dazzleduck.sql.commons.ingestion.PostIngestionTask
            createPostIngestionTask(io.dazzleduck.sql.commons.ingestion.IngestionResult r) {
                return io.dazzleduck.sql.commons.ingestion.PostIngestionTask.NOOP;
            }
            // The handler is the source of truth for routable queues; these tests use the standard
            // signal queue IDs.
            @Override public java.util.Set<String> getKnownQueues() {
                return java.util.Set.of("logs", "traces", "metrics");
            }
            @Override public String getTargetPath(String id) { return outputPath; }
            @Override public String[] getPartitionBy(String id) { return new String[0]; }
            @Override public String getTransformation(String id) { return transformation; }
        };
    }

    private static io.dazzleduck.sql.commons.ingestion.IngestionConfig ingestionConfig(
            long minBucketSize, long maxDelayMs) {
        return new io.dazzleduck.sql.commons.ingestion.IngestionConfig(
                minBucketSize,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_BATCHES,
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                java.time.Duration.ofMillis(maxDelayMs),
                io.dazzleduck.sql.commons.ingestion.IngestionConfig.DEFAULT_CONFIG_REFRESH);
    }
}
