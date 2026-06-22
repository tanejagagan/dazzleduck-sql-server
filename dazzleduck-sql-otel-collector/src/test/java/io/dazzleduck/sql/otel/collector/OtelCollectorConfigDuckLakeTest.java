package io.dazzleduck.sql.otel.collector;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.jsonwebtoken.Jwts;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.SeverityNumber;
import org.junit.jupiter.api.*;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end integration test that exercises the full HOCON config path:
 * {@code CollectorConfig} → unified {@code ingestion_task_factory_provider} block
 * → {@code DuckLakeIngestionTaskFactoryProvider} → DuckLake.
 *
 * <p>This test verifies that the top-level {@code ingestion_task_factory_provider}
 * with {@code ingestion_queue_table_mapping} entries is correctly resolved and
 * that data written via OTLP is registered in the DuckLake catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OtelCollectorConfigDuckLakeTest {

    private static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String CATALOG = "otel_config_test";

    Path tempDir;

    OtelCollectorServer otelServer;
    ManagedChannel channel;
    LogsServiceGrpc.LogsServiceBlockingStub logsStub;

    @BeforeAll
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("otel-config-test");
        Path dataDir  = tempDir.resolve("data");
        Path logsDir  = tempDir.resolve("logs");
        Path catalogDb = tempDir.resolve("catalog");
        Files.createDirectories(dataDir);
        Files.createDirectories(logsDir);

        // Set up DuckLake catalog.
        ConnectionPool.executeBatch(new String[]{"INSTALL ducklake", "LOAD ducklake", "LOAD arrow"});
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(catalogDb, CATALOG, dataDir),
                    "CREATE TABLE %s.main.logs (severity_number INT, severity_text VARCHAR, body VARCHAR)"
                            .formatted(CATALOG)
            });
        }

        // Output directory provisioning is the operator's responsibility (ParquetIngestionQueue no
        // longer creates it); the test is the operator and pre-creates the logs table's data dir.
        Files.createDirectories(dataDir.resolve("main").resolve("logs"));

        // Build the collector config entirely from HOCON — exercises the unified
        // ingestion_task_factory_provider block with per-signal mapping entries.
        int port = freePort();
        String hocon = """
                otel_collector {
                    grpc_port = %d
                    startup_script = "LOAD arrow;"
                    authentication = "jwt"
                    secret_key = "%s"
                    users = [{ username = admin, password = admin }]

                    ingestion_task_factory_provider {
                        class = "io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider"
                        queue_config_refresh_delay_ms = 120000
                        ingestion_queue_table_mapping = [
                            {
                                ingestion_queue = "logs"
                                output_path     = "%s"
                                partition_by    = []
                                min_bucket_size = 1
                                max_delay_ms    = 60000
                                catalog = "%s"
                                schema  = "main"
                                table   = "logs"
                            }
                            {
                                ingestion_queue = "traces"
                                output_path     = "%s"
                            }
                            {
                                ingestion_queue = "metrics"
                                output_path     = "%s"
                            }
                        ]
                    }
                }
                """.formatted(
                        port, SECRET_KEY_BASE64,
                        logsDir,
                        CATALOG,
                        tempDir.resolve("traces"),
                        tempDir.resolve("metrics"));

        var collectorConfig = new CollectorConfig(ConfigFactory.parseString(hocon));
        otelServer = new OtelCollectorServer(collectorConfig.toProperties());
        otelServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        logsStub = LogsServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearerMeta("logs")));
    }

    @AfterAll
    void cleanup() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (otelServer != null) otelServer.close();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void logRecord_viaConfigProvider_registeredInDuckLake() throws Exception {
        logsStub.export(ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                        .setSeverityText("INFO")
                                        .setBody(AnyValue.newBuilder()
                                                .setStringValue("config-driven test").build())
                                        .build())
                                .build())
                        .build())
                .build());

        // severity_number = 9 for INFO in OTLP
        TestUtils.isEqual(
                "SELECT 9 AS severity_number, 'INFO' AS severity_text, 'config-driven test' AS body",
                "SELECT severity_number, severity_text, body FROM %s.main.logs".formatted(CATALOG));
    }

    @Test
    void collectorConfig_resolves_ingestionTaskFactoryProvider() throws Exception {
        // Verifies CollectorConfig correctly reads the unified ingestion_task_factory_provider
        // block and returns a DuckLakeIngestionHandler (not the NOOP fallback).
        int port = freePort();
        String hocon = """
                otel_collector {
                    grpc_port = %d
                    ingestion_task_factory_provider {
                        class = "io.dazzleduck.sql.commons.ingestion.DuckLakeIngestionTaskFactoryProvider"
                        queue_config_refresh_delay_ms = 120000
                        ingestion_queue_table_mapping = [
                            {
                                ingestion_queue = "logs"
                                output_path     = "%s"
                                catalog = "%s"
                                schema  = "main"
                                table   = "logs"
                            }
                            {
                                ingestion_queue = "traces"
                                output_path     = "%s"
                            }
                            {
                                ingestion_queue = "metrics"
                                output_path     = "%s"
                            }
                        ]
                    }
                }
                """.formatted(port, tempDir.resolve("logs2"), CATALOG,
                              tempDir.resolve("traces2"), tempDir.resolve("metrics2"));

        var cfg = new CollectorConfig(ConfigFactory.parseString(hocon));
        var handler = cfg.getIngestionHandler();

        // DuckLakeIngestionHandler returns the table's target path; NOOP handler returns the outputPath.
        // The DuckLake table path starts with dataDir (the catalog's DATA_PATH).
        String targetPath = handler.getTargetPath("logs");
        assertEquals(
                tempDir.resolve("data").toString(),
                targetPath != null ? targetPath.substring(0, tempDir.resolve("data").toString().length()) : null,
                "Expected DuckLakeIngestionHandler to return DuckLake data path, not NOOP output path");
    }

    // -----------------------------------------------------------------------

    private static int freePort() throws IOException {
        try (var s = new ServerSocket(0)) { s.setReuseAddress(true); return s.getLocalPort(); }
    }

    private static Metadata bearerMeta(String queueId) {
        SecretKey key = Validator.fromBase64String(SECRET_KEY_BASE64);
        Calendar exp = Calendar.getInstance();
        exp.add(Calendar.HOUR, 1);
        String token = Jwts.builder()
                .subject("admin")
                .claim(io.dazzleduck.sql.common.Headers.CLAIM_INGESTION_QUEUE, queueId)
                .expiration(exp.getTime())
                .signWith(key)
                .compact();
        var meta = new Metadata();
        meta.put(AUTHORIZATION_KEY, "Bearer " + token);
        return meta;
    }
}
