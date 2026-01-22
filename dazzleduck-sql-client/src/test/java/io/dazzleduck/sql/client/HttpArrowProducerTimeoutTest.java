package io.dazzleduck.sql.client;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.runtime.SharedTestServer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class HttpArrowProducerTimeoutTest {

    private static SharedTestServer server;
    private static String warehouse;
    private static String baseUrl;
    private static Schema schema;

    @BeforeAll
    static void setup() throws Exception {
        server = new SharedTestServer();
        server.start();
        warehouse = server.getWarehousePath();
        baseUrl = server.getHttpBaseUrl();
        schema = new Schema(java.util.List.of(new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    @AfterAll
    static void teardown() {
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    void setupEach() {
        org.awaitility.Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
        org.awaitility.Awaitility.setDefaultTimeout(2, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardownEach() {
        org.awaitility.Awaitility.reset();
    }

    private HttpArrowProducer newSender(String file, Duration timeout) {
        return new HttpArrowProducer(
                schema,
                baseUrl,
                "admin",
                "admin",
                Map.of(),
                file,
                timeout,
                100_000,
                200_000,
                Duration.ofMillis(200),
                3,
                1000,
                java.util.List.of(),
                java.util.List.of(),
                100,
                100_000,
                Clock.systemDefaultZone()
        );
    }

    private byte[] arrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection conn = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(conn, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return baos.toByteArray();
        }
    }

    @Test
    void testTimeoutFailure() throws Exception {
        String file = "timeout-" + System.nanoTime() + ".parquet";

        // Test that ingestion to non-existent directory fails as expected
        try (var timeoutSender = newSender(file, Duration.ofSeconds(5))) {
            timeoutSender.enqueue(arrowBytes("select * from generate_series(2000)"));
            await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().untilAsserted(() ->
                    assertThrows(Exception.class, () ->
                            ConnectionPool.collectFirst(String.format("select count(*) from read_parquet('%s/%s')", warehouse, file), Long.class)
                    )
            );
        }
    }
}
