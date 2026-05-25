package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Shared resources and common logic for OTLP signal services.
 * Each signal service holds one instance as a field and delegates to these methods.
 */
class OtelServiceBase implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelServiceBase.class);

    final RootAllocator allocator;
    final Path tempDir;

    OtelServiceBase(String tempDirPrefix) throws IOException {
        this.allocator = new RootAllocator();
        this.tempDir = Files.createTempDirectory(tempDirPrefix);
    }

    /**
     * Reads the {@value Headers#CLAIM_INGESTION_QUEUE} JWT claim from the gRPC context and
     * resolves it to a {@link SignalWriter}. Returns {@code null} after sending an
     * {@code INVALID_ARGUMENT} error if the claim is absent or the queue is not configured.
     */
    static SignalWriter resolveWriter(Map<String, SignalWriter> writers,
                                      StreamObserver<?> responseObserver) {
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        if (queueId == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing required JWT claim: '" + Headers.CLAIM_INGESTION_QUEUE + "'")
                    .asRuntimeException());
            return null;
        }
        SignalWriter writer = writers.get(queueId);
        if (writer == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Unknown ingestion queue: '" + queueId + "'")
                    .asRuntimeException());
            return null;
        }
        return writer;
    }

    /**
     * Serialises {@code entries} to a temporary Arrow IPC file using the provided schema
     * and batch writer. Deletes the partial file before rethrowing on any failure.
     */
    <E> Path writeArrowFile(List<E> entries, Schema schema,
                             BiConsumer<List<E>, VectorSchemaRoot> batchWriter) throws IOException {
        Path tempFile = tempDir.resolve("batch_" + UUID.randomUUID() + ".arrow");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            batchWriter.accept(entries, root);
            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(fos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        } catch (Exception e) {
            Files.deleteIfExists(tempFile);
            if (e instanceof IOException ioe) throw ioe;
            throw new IOException(e);
        }
        return tempFile;
    }

    /**
     * Returns a {@code whenComplete} handler for a {@link io.dazzleduck.sql.otel.collector.SignalWriter#addBatch}
     * future. On success records the export metric and notifies the client; on failure deletes
     * the temp file, records the error metric, and propagates the error to the client.
     */
    static <R> BiConsumer<Void, Throwable> batchCompleteHandler(
            Path arrowFile,
            int count,
            String queueId,
            Timer.Sample sample,
            OtelCollectorMetrics metrics,
            StreamObserver<R> responseObserver,
            R successResponse) {
        return (v, ex) -> {
            if (ex != null) {
                try { Files.deleteIfExists(arrowFile); } catch (IOException ignored) {}
                metrics.recordError(queueId, sample);
                log.error("Failed to persist {} records for queue '{}'", count, queueId, ex);
                responseObserver.onError(ex);
            } else {
                metrics.recordExport(queueId, count, sample);
                responseObserver.onNext(successResponse);
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void close() {
        try {
            allocator.close();
        } catch (Exception e) {
            log.warn("Error closing Arrow allocator (prefix={})", tempDir.getFileName(), e);
        }
        try (var stream = Files.walk(tempDir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }
}
