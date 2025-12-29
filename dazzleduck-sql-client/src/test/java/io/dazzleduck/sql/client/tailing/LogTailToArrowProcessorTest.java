package io.dazzleduck.sql.client.tailing;

import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class LogTailToArrowProcessorTest {

    static final int PORT = 8094;
    static String warehouse;
    static Schema schema;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        // ------------------------------------------------------------
        // 1. Create warehouse
        // ------------------------------------------------------------
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        new java.io.File(warehouse).mkdirs();

        // ------------------------------------------------------------
        // 2. Start DazzleDuck HTTP server
        // ------------------------------------------------------------
        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehouse,
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"
        });

        // ------------------------------------------------------------
        // 3. Schema (same pattern as HttpSenderTest)
        // ------------------------------------------------------------
        JsonToArrowConverter converter = new JsonToArrowConverter();
        schema = converter.getSchema();
        converter.close();
    }

    @BeforeEach
    void setupEach() {
        org.awaitility.Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
        org.awaitility.Awaitility.setDefaultTimeout(3, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardownEach() {
        org.awaitility.Awaitility.reset();
    }

    @Test
    void endToEnd_logFile_to_arrow_to_server() throws Exception {
        // ------------------------------------------------------------
        // 1. Create temp log file
        // ------------------------------------------------------------
        Path logFile = tempDir.resolve("app.log");
        Files.writeString(logFile, """
            {"timestamp":"2024-01-01T10:00:00Z","level":"INFO","thread":"main","logger":"App","message":"Hello"}
            {"timestamp":"2024-01-01T10:00:01Z","level":"WARN","thread":"main","logger":"App","message":"World"}
            """);

        String parquetFile = "log-tail-" + System.nanoTime() + ".parquet";

        // ------------------------------------------------------------
        // 2. Create REAL HttpSender
        // ------------------------------------------------------------
        try (HttpSender sender = new HttpSender(
                schema,
                "http://localhost:" + PORT,
                "admin",
                "admin",
                parquetFile,
                Duration.ofSeconds(5),
                1,
                Duration.ofSeconds(1),
                10_000_000,
                10_000_000
        )) {

            // ------------------------------------------------------------
            // 3. Start processor
            // ------------------------------------------------------------
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(logFile.toString(), sender, 100);

            processor.start();

            // ------------------------------------------------------------
            // 4. Verify ingestion via DuckDB
            // ------------------------------------------------------------
            await().ignoreExceptions().untilAsserted(() -> {
                        long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s')".formatted(warehouse, parquetFile), Long.class);
                        assertEquals(2, count);
                    });
            processor.close();
        }
    }
}
