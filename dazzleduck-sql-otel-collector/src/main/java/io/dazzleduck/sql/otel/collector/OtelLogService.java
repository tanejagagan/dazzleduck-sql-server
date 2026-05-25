package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * gRPC service that receives OTLP log exports and writes them to Parquet.
 * Queue routing is driven by the {@code x-dd-ingestion-queue} JWT claim.
 */
public class OtelLogService extends LogsServiceGrpc.LogsServiceImplBase implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelLogService.class);

    private final Map<String, SignalWriter> writers;
    private final OtelCollectorMetrics metrics;
    private final OtelServiceBase base;

    public OtelLogService(Map<String, SignalWriter> writers, OtelCollectorMetrics metrics) throws IOException {
        this.writers = writers;
        this.metrics = metrics;
        this.base = new OtelServiceBase("otel-logs-arrow-");
    }

    @Override
    public void export(ExportLogsServiceRequest request,
                       StreamObserver<ExportLogsServiceResponse> responseObserver) {
        SignalWriter writer = OtelServiceBase.resolveWriter(writers, responseObserver);
        if (writer == null) return;

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
        int recordCount = entries.size();
        String queueId = JwtServerInterceptor.QUEUE_CONTEXT_KEY.get();
        log.debug("Received {} log records → queue '{}'", recordCount, queueId);
        var sample = metrics.startSample();

        try {
            Path arrowFile = base.writeArrowFile(entries, OtelLogSchema.SCHEMA, LogRecordBatchWriter::write);
            writer.addBatch(arrowFile).whenComplete(
                    OtelServiceBase.batchCompleteHandler(arrowFile, recordCount, queueId,
                            sample, metrics,
                            responseObserver, ExportLogsServiceResponse.getDefaultInstance()));
        } catch (IOException e) {
            metrics.recordError(queueId, sample);
            log.error("Failed to write Arrow file for {} log records", recordCount, e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void close() {
        base.close();
    }
}
