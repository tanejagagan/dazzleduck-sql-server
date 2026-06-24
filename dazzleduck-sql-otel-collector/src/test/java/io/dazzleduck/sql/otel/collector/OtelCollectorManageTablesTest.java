package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.ingestion.DynamicIngestionHandler;
import io.dazzleduck.sql.commons.ingestion.DynamicQueueRepository;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.QueueIdToTableMapping;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end test for {@code manage_tables}: the collector is given a SQLite registry row with an
 * {@code input_schema} + {@code transformation} but <strong>no pre-created DuckLake table</strong>.
 * It must (a) auto-create the table with the derived columns, and (b) ingest OTLP logs so the data —
 * including the transformation's derived column — shows up in that auto-created table.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OtelCollectorManageTablesTest {

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";
    static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    static final String CATALOG = "otel_manage_tables_test";
    static final String QUEUE = "logs";          // JWT ingestion-queue claim / registry key
    static final String TABLE = "app_logs";      // target DuckLake table (auto-created)

    OtelCollectorServer otelServer;
    DynamicIngestionHandler handler;
    ManagedChannel channel;
    LogsServiceGrpc.LogsServiceBlockingStub logsStub;

    @BeforeAll
    void setup() throws Exception {
        Path tempDir = Files.createTempDirectory("otel-manage-tables-test");
        Path dataDir = tempDir.resolve("data");
        Files.createDirectories(dataDir);
        // Operator provisions the table's data directory (ParquetIngestionQueue never creates it).
        Files.createDirectories(dataDir.resolve("main").resolve(TABLE));

        // Attach the catalog and create the schema — but NOT the table; manage_tables creates it.
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, dataDir),
                    "CREATE SCHEMA IF NOT EXISTS %s.main".formatted(CATALOG)
            });
        }

        // SQLite registry: one queue, mapped to TABLE, with an input_schema + a transformation that
        // adds a 'level' column. No DuckLake table exists yet.
        String dbPath = tempDir.resolve("ingestion-queues.db").toString();
        DynamicQueueRepository repo = new DynamicQueueRepository(dbPath);
        repo.init();
        writeRegistry(dbPath,
                "INSERT INTO reg.ingestion_queues " +
                "(ingestion_queue, catalog, schema_name, table_name, transformation, input_schema) VALUES " +
                "('%s', '%s', 'main', '%s', 'SELECT *, severity_text AS level FROM __this', %s)"
                        .formatted(QUEUE, CATALOG, TABLE,
                                "'severity_number INTEGER, severity_text VARCHAR, body VARCHAR'"));

        Connection readConn = repo.openReadOnlyConnection();
        Map<String, QueueIdToTableMapping> initial = DynamicQueueRepository.loadAll(readConn);
        // manage_tables=true → the constructor reconciles the mapping and CREATEs the table now.
        handler = new DynamicIngestionHandler(dbPath, readConn, initial, Duration.ofSeconds(60), true);

        CollectorProperties props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO); // no LB-drain wait in tests
        props.setGrpcPort(freePort());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        props.setStartupScript("LOAD arrow;");
        props.setIngestionHandler(handler);
        props.setIngestionConfig(new IngestionConfig(1L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE,
                IngestionConfig.DEFAULT_MAX_BATCHES, IngestionConfig.DEFAULT_MAX_PENDING_WRITE,
                Duration.ofSeconds(60), IngestionConfig.DEFAULT_CONFIG_REFRESH));

        otelServer = new OtelCollectorServer(props);
        otelServer.start();

        channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
        logsStub = LogsServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearerMeta(QUEUE)));
    }

    @AfterAll
    void cleanup() throws Exception {
        if (channel != null) {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (otelServer != null) otelServer.close();
        if (handler != null) handler.closeQueues();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void manageTables_createsTableFromInputSchemaThenIngestsData() throws Exception {
        // (a) the table was auto-created at handler construction, with the transformation's columns.
        assertEquals(List.of("severity_number", "severity_text", "body", "level"), tableColumns());

        // (b) send an OTLP log and verify it lands in the auto-created table, with 'level' populated.
        logsStub.export(ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setSeverityNumber(SeverityNumber.SEVERITY_NUMBER_INFO)
                                        .setSeverityText("INFO")
                                        .setBody(AnyValue.newBuilder().setStringValue("hello manage").build())
                                        .build())
                                .build())
                        .build())
                .build());

        awaitRowCount("SELECT count(*) FROM %s.main.%s WHERE body = 'hello manage'".formatted(CATALOG, TABLE), 1);

        try (Connection conn = ConnectionPool.getConnection()) {
            var row = ConnectionPool.collectAll(conn,
                    "SELECT severity_number, severity_text, body, level FROM %s.main.%s WHERE body = 'hello manage'"
                            .formatted(CATALOG, TABLE),
                    rs -> rs.getInt("severity_number") + "|" + rs.getString("severity_text")
                            + "|" + rs.getString("body") + "|" + rs.getString("level"));
            List<String> rows = new ArrayList<>();
            row.forEach(rows::add);
            assertEquals(1, rows.size());
            assertEquals("9|INFO|hello manage|INFO", rows.get(0),
                    "derived 'level' column must equal severity_text via the transformation");
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<String> tableColumns() throws Exception {
        try (Connection conn = ConnectionPool.getConnection()) {
            List<String> cols = new ArrayList<>();
            ConnectionPool.collectAll(conn,
                    "SELECT column_name FROM information_schema.columns WHERE table_catalog = '%s' "
                            .formatted(CATALOG)
                            + "AND table_schema = 'main' AND table_name = '%s' ORDER BY ordinal_position"
                            .formatted(TABLE),
                    rs -> rs.getString("column_name")).forEach(cols::add);
            return cols;
        }
    }

    private void awaitRowCount(String countSql, long expected) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        long last = -1;
        while (System.nanoTime() < deadline) {
            try (Connection conn = ConnectionPool.getConnection()) {
                Long c = ConnectionPool.collectFirst(conn, countSql, Long.class);
                last = c != null ? c : -1;
                if (last == expected) return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Expected " + expected + " row(s) but got " + last + " for: " + countSql);
    }

    private static void writeRegistry(String dbPath, String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '" + dbPath.replace("'", "''") + "' AS reg (TYPE sqlite)");
            st.execute(sql);
        }
    }

    private static int freePort() throws IOException {
        try (var s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
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
