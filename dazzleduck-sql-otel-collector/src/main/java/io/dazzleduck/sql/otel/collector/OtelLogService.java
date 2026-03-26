package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC service that receives OTLP log exports and writes them to Parquet.
 */
public class OtelLogService extends LogsServiceGrpc.LogsServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelLogService.class);

    private final SignalWriter logWriter;

    public OtelLogService(SignalWriter logWriter) {
        this.logWriter = logWriter;
    }

    @Override
    public void export(ExportLogsServiceRequest request,
                       StreamObserver<ExportLogsServiceResponse> responseObserver) {
        List<JavaRow> rows = new ArrayList<>();
        for (ResourceLogs resourceLogs : request.getResourceLogsList()) {
            var resource = resourceLogs.hasResource() ? resourceLogs.getResource() : null;
            for (ScopeLogs scopeLogs : resourceLogs.getScopeLogsList()) {
                var scope = scopeLogs.hasScope() ? scopeLogs.getScope() : null;
                for (LogRecord record : scopeLogs.getLogRecordsList()) {
                    rows.add(LogRecordConverter.toRow(record, resource, scope));
                }
            }
        }
        log.debug("Received {} log records", rows.size());
        logWriter.addBatch(rows).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to persist {} log records", rows.size(), ex);
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(ExportLogsServiceResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        });
    }
}
