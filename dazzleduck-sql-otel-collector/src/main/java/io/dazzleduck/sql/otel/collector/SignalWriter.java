package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.common.types.VectorSchemaRootWriter;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Buffers OTLP signal rows (logs, metrics, or traces) and flushes them to Parquet via DuckDB.
 *
 * Flush is triggered when:
 * - buffer size reaches flushThreshold, or
 * - flushIntervalMs elapses since last flush
 *
 * The response future returned by {@link #addBatch} completes only after the Parquet write
 * succeeds (or fails), so callers can back-pressure the gRPC response accordingly.
 */
public class SignalWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SignalWriter.class);

    private final Schema schema;
    private final String outputPath;
    private final List<String> partitionBy;
    private final String transformations;
    private final int flushThreshold;
    private final long flushIntervalMs;

    private final List<JavaRow> buffer = new ArrayList<>();
    private final List<CompletableFuture<Void>> pendingFutures = new ArrayList<>();
    private final VectorSchemaRootWriter schemaWriter;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong totalWritten = new AtomicLong(0);
    private final AtomicLong totalDropped = new AtomicLong(0);
    private volatile long lastFlushTime = System.currentTimeMillis();

    public SignalWriter(Schema schema, String outputPath, List<String> partitionBy,
                        String transformations, int flushThreshold, long flushIntervalMs) {
        this.schema = schema;
        this.outputPath = outputPath;
        this.partitionBy = partitionBy;
        this.transformations = transformations;
        this.flushThreshold = flushThreshold;
        this.flushIntervalMs = flushIntervalMs;
        this.schemaWriter = VectorSchemaRootWriter.of(schema);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otel-signal-flusher[" + outputPath + "]");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::checkAndFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Adds a batch of rows to the buffer and returns a future that completes
     * when the flush containing these rows finishes writing to Parquet.
     * The future completes exceptionally if the write fails.
     */
    public synchronized CompletableFuture<Void> addBatch(List<JavaRow> rows) {
        if (rows.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        buffer.addAll(rows);
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingFutures.add(future);
        if (buffer.size() >= flushThreshold) {
            flush();
        }
        return future;
    }

    private void checkAndFlush() {
        long elapsed = System.currentTimeMillis() - lastFlushTime;
        if (elapsed >= flushIntervalMs) {
            synchronized (this) {
                if (!buffer.isEmpty()) {
                    flush();
                }
            }
        }
    }

    /** Must be called with {@code this} lock held. */
    private void flush() {
        if (buffer.isEmpty()) return;

        List<JavaRow> rows = new ArrayList<>(buffer);
        List<CompletableFuture<Void>> futures = new ArrayList<>(pendingFutures);
        buffer.clear();
        pendingFutures.clear();
        lastFlushTime = System.currentTimeMillis();

        try {
            writeToParquet(rows);
            totalWritten.addAndGet(rows.size());
            log.debug("Flushed {} rows to {}", rows.size(), outputPath);
            futures.forEach(f -> f.complete(null));
        } catch (Exception e) {
            totalDropped.addAndGet(rows.size());
            log.error("Failed to flush {} rows to parquet at {}", rows.size(), outputPath, e);
            futures.forEach(f -> f.completeExceptionally(e));
        }
    }

    private void writeToParquet(List<JavaRow> rows) throws Exception {
        JavaRow[] rowArray = rows.toArray(new JavaRow[0]);

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            schemaWriter.writeToVector(rowArray, root);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            try (ArrowStreamReader reader = new ArrowStreamReader(
                    new ByteArrayInputStream(baos.toByteArray()), allocator)) {
                ConnectionPool.bulkIngestToFile(reader, allocator, outputPath, partitionBy, "parquet", transformations);
            }
        }
    }

    public long getTotalWritten() {
        return totalWritten.get();
    }

    public long getTotalDropped() {
        return totalDropped.get();
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            if (!buffer.isEmpty()) {
                flush();
            }
            var ex = new RuntimeException("SignalWriter closed before flush completed");
            pendingFutures.forEach(f -> f.completeExceptionally(ex));
            pendingFutures.clear();
        }
        log.info("SignalWriter[{}] closed — written={}, dropped={}", outputPath, totalWritten.get(), totalDropped.get());
    }
}
