package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionResult;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTask;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A batch the queue has accepted belongs to the queue, even if its caller stops waiting for it.
 * Helidon interrupts a request's thread when its HTTP/2 connection closes; the ingest handler used
 * to delete the batch's staged file then, while the batch was still queued. The write of its bucket
 * then failed with "No files found" and the bucket was dropped: the accepted batch was lost, and
 * so was every other client's batch written in the same bucket.
 */
class IngestInterruptedWaitTest {

    private static final String QUEUE = "interrupted_wait";

    @BeforeAll
    static void loadArrow() {
        // Queued batches are read back with read_arrow.
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
    }

    private static final class Ack implements FlightProducer.StreamListener<PutResult> {
        final CompletableFuture<Throwable> done = new CompletableFuture<>();
        @Override public void onNext(PutResult val) { }
        @Override public void onError(Throwable t) { done.complete(t); }
        @Override public void onCompleted() { done.complete(null); }
    }

    private static byte[] arrowRows(int rows) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             DuckDBConnection connection = ConnectionPool.getConnection();
             var reader = ConnectionPool.getReader(connection, allocator, "SELECT * FROM range(%d)".formatted(rows), 1000);
             var out = new ByteArrayOutputStream();
             var writer = new ArrowStreamWriter(reader.getVectorSchemaRoot(), null, out)) {
            writer.start();
            while (reader.loadNextBatch()) {
                writer.writeBatch();
            }
            writer.end();
            return out.toByteArray();
        }
    }

    private static Runnable ingest(DuckDBFlightSqlProducer producer, String producerId, byte[] arrow, Ack ack) {
        var context = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
        var params = new IngestionParameters(QUEUE, "parquet", new String[0], new String[0], producerId, 1L, Map.of());
        return producer.acceptPutStatementBulkIngest(context, params, new ByteArrayInputStream(arrow), ack);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void anInterruptedWaitKeepsTheAcceptedBatch(@TempDir Path output) throws Exception {
        IngestionHandler handler = new IngestionHandler() {
            @Override public PostIngestionTask createPostIngestionTask(IngestionResult result) { return PostIngestionTask.NOOP; }
            @Override public String getTargetPath(String queueId) { return output.toString(); }
            @Override public String[] getPartitionBy(String queueId) { return new String[0]; }
        };
        // Batches wait up to 2 s for the flush, long enough to interrupt one that is queued.
        var config = new IngestionConfig(Long.MAX_VALUE, 1024 * 1024 * 1024L, 2048, 256 * 1024 * 1024L,
                Duration.ofSeconds(2));
        var producer = new DuckDBFlightSqlProducer(
                FlightTestUtils.findNextLocation(), UUID.randomUUID().toString(), "secret", new RootAllocator(),
                System.getProperty("java.io.tmpdir"), AccessMode.COMPLETE, DuckDBFlightSqlProducer.newTempDir(), handler,
                Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(1), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                config, List.of());
        try {
            byte[] batch = arrowRows(10);

            // Client A: accepted and waiting for the flush, then interrupted (its client went away).
            var ackA = new Ack();
            var a = new Thread(ingest(producer, "a", batch, ackA), "client-a");
            a.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (a.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                Thread.sleep(10); // until it waits on the batch's result
            }
            assertEquals(Thread.State.TIMED_WAITING, a.getState(), "client A waits for its batch");
            a.interrupt();
            assertNotNull(ackA.done.get(10, TimeUnit.SECONDS), "client A's wait fails");

            // Client B, same queue: its batch is written.
            var ackB = new Ack();
            ingest(producer, "b", batch, ackB).run();
            assertNull(ackB.done.get(10, TimeUnit.SECONDS), "client B's batch is written");

            // A's batch was accepted, so it is written too. (Without the fix its bucket's write failed
            // with "No files found" and was dropped, leaving only B's 10 rows.)
            long rows = ConnectionPool.collectFirst(
                    "SELECT count(*) FROM read_parquet('%s/**/*.parquet')".formatted(output), Long.class);
            assertEquals(20, rows);
        } finally {
            producer.close();
        }
    }
}
