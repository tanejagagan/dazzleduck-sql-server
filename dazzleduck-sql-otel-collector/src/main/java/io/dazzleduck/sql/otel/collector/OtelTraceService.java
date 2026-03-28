package io.dazzleduck.sql.otel.collector;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * gRPC service that receives OTLP trace exports and writes them to Parquet.
 */
public class OtelTraceService extends TraceServiceGrpc.TraceServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelTraceService.class);

    private final SignalWriter writer;
    private final Path tempDir;
    private final OtelCollectorMetrics metrics;

    public OtelTraceService(SignalWriter writer, OtelCollectorMetrics metrics) throws IOException {
        this.writer = writer;
        this.metrics = metrics;
        this.tempDir = Files.createTempDirectory("otel-traces-arrow-");
    }

    @Override
    public void export(ExportTraceServiceRequest request,
                       StreamObserver<ExportTraceServiceResponse> responseObserver) {
        List<SpanEntry> entries = new ArrayList<>();
        for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
            var resource = resourceSpans.hasResource() ? resourceSpans.getResource() : null;
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                var scope = scopeSpans.hasScope() ? scopeSpans.getScope() : null;
                for (var span : scopeSpans.getSpansList()) {
                    entries.add(new SpanEntry(span, resource, scope));
                }
            }
        }
        log.debug("Received {} spans", entries.size());
        var sample = metrics.startSample();

        try {
            Path arrowFile = writeArrowFile(entries);
            writer.addBatch(arrowFile).whenComplete((v, ex) -> {
                if (ex != null) {
                    metrics.recordTraceError(sample);
                    log.error("Failed to persist {} spans", entries.size(), ex);
                    responseObserver.onError(ex);
                } else {
                    metrics.recordTraceExport(entries.size(), sample);
                    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            });
        } catch (IOException e) {
            metrics.recordTraceError(sample);
            log.error("Failed to write Arrow file for {} spans", entries.size(), e);
            responseObserver.onError(e);
        }
    }

    private Path writeArrowFile(List<SpanEntry> entries) throws IOException {
        Path tempFile = tempDir.resolve("batch_" + UUID.randomUUID() + ".arrow");
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelTraceSchema.SCHEMA, allocator)) {
            SpanBatchWriter.write(entries, root);
            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(fos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        }
        return tempFile;
    }
}
