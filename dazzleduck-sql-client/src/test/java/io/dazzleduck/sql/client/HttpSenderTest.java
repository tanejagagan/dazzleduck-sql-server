package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.http.server.Main;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.ByteArrayOutputStream;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HttpSenderTest {

    static final int PORT = 8093;
    static String warehouse;
    static ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void setup() throws Exception {
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        new java.io.File(warehouse).mkdir();

        Main.main(new String[]{
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehouse
        });

        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow"
        });
    }

    private byte[] arrowBytes(String query) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection conn = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(conn, allocator, query, 1000);
             var baos = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, baos)) {

            writer.start();
            while (reader.loadNextBatch()) writer.writeBatch();
            writer.end();
            return baos.toByteArray();
        }
    }

    @Test
    @Order(1)
    void testAsyncIngestionSingleBatch() throws Exception {

        var sender = new HttpSender(
                "http://localhost:" + PORT,
                "admin",
                "admin",
                "single.arrow",
                Duration.ofSeconds(10),
                10_000,
                100_000
        );

        sender.start();
        sender.enqueue(arrowBytes("select * from generate_series(4)"));

        // WAIT for async send to complete
        Thread.sleep(200);

        long count = ConnectionPool.collectFirst(
                "select count(*) from read_parquet('%s/single.arrow')".formatted(warehouse),
                Long.class
        );

        assertEquals(5, count);
    }

    @Test
    @Order(2)
    void testMultipleEnqueuesOverwriteBehavior() throws Exception {

        var sender = new HttpSender(
                "http://localhost:" + PORT,
                "admin",
                "admin",
                "multi.parquet",
                Duration.ofSeconds(10),
                50_000,
                200_000
        );

        sender.start();

        sender.enqueue(arrowBytes("select * from generate_series(2)")); // 3 rows
        sender.enqueue(arrowBytes("select * from generate_series(3)")); // 4 rows overwrites

        Thread.sleep(2000);

        long count = ConnectionPool.collectFirst(
                "select count(*) from read_parquet('%s/*.parquet')".formatted(warehouse),
                Long.class
        );

        assertEquals(4, count);
    }

    @Test
    void testMemoryDiskSwitching() throws Exception {

        long tinyMemLimit = 50; // force spill immediately
        var sender = new HttpSender(
                "http://localhost:" + PORT,
                "admin",
                "admin",
                "spill.parquet",
                Duration.ofSeconds(10),
                tinyMemLimit,
                100_000
        );

        sender.start();
        sender.enqueue(arrowBytes("select * from generate_series(30)")); // large batch spills to file

        Thread.sleep(2000);

        long count = ConnectionPool.collectFirst(
                "select count(*) from read_parquet('%s/spill.parquet')".formatted(warehouse),
                Long.class
        );

        assertEquals(31, count);
    }


    @Test
    @Order(5)
    void testTimeoutFailure() throws Exception {

        var sender = new HttpSender(
                "http://localhost:" + PORT,
                "admin",
                "admin",
                "timeout.parquet",
                Duration.ofMillis(1), // extreme low timeout
                5_000,
                50_000
        );

        sender.start();
        sender.enqueue(arrowBytes("select * from generate_series(2000)"));

        Thread.sleep(1500);

        // file should not exist or cannot be read successfully
        assertThrows(Exception.class, () ->
                ConnectionPool.collectFirst(
                        "select count(*) from read_parquet('%s/timeout.parquet')".formatted(warehouse),
                        Long.class
                )
        );
    }

}
