package io.dazzleduck.sql.otel.collector;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
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
 * gRPC service that receives OTLP log exports and writes them to Parquet.
 */
public class OtelLogService extends LogsServiceGrpc.LogsServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelLogService.class);

    private final SignalWriter logWriter;
    private final Path tempDir;
    private final OtelCollectorMetrics metrics;

    public OtelLogService(SignalWriter logWriter, OtelCollectorMetrics metrics) throws IOException {
        this.logWriter = logWriter;
        this.metrics = metrics;
        this.tempDir = Files.createTempDirectory("otel-logs-arrow-");
    }

    @Override
    public void export(ExportLogsServiceRequest request,
                       StreamObserver<ExportLogsServiceResponse> responseObserver) {
        List<LogEntry> entries = new ArrayList<>();
        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            var resource = resourceLogs.hasResource() ? resourceLogs.getResource() : null;
            for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                var scope = scopeLogs.hasScope() ? scopeLogs.getScope() : null;
                for (var record : scopeLogs.getLogRecordsList()) {
                    entries.add(new LogEntry(record, resource, scope));
                }
            }
        }
        log.debug("Received {} log records", entries.size());
        var sample = metrics.startSample();

        try {
            Path arrowFile = writeArrowFile(entries);
            logWriter.addBatch(arrowFile).whenComplete((v, ex) -> {
                if (ex != null) {
                    metrics.recordLogError(sample);
                    log.error("Failed to persist {} log records", entries.size(), ex);
                    responseObserver.onError(ex);
                } else {
                    metrics.recordLogExport(entries.size(), sample);
                    responseObserver.onNext(ExportLogsServiceResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            });
        } catch (IOException e) {
            metrics.recordLogError(sample);
            log.error("Failed to write Arrow file for {} log records", entries.size(), e);
            responseObserver.onError(e);
        }
    }

    private Path writeArrowFile(List<LogEntry> entries) throws IOException {
        Path tempFile = tempDir.resolve("batch_" + UUID.randomUUID() + ".arrow");
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelLogSchema.SCHEMA, allocator)) {
            LogRecordBatchWriter.write(entries, root);
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
