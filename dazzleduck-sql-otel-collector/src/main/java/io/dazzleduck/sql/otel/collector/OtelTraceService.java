package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC service that receives OTLP trace exports and writes them to Parquet.
 */
public class OtelTraceService extends TraceServiceGrpc.TraceServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelTraceService.class);

    private final SignalWriter writer;

    public OtelTraceService(SignalWriter writer) {
        this.writer = writer;
    }

    @Override
    public void export(ExportTraceServiceRequest request,
                       StreamObserver<ExportTraceServiceResponse> responseObserver) {
        List<JavaRow> rows = new ArrayList<>();
        for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
            var resource = resourceSpans.hasResource() ? resourceSpans.getResource() : null;
            for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
                var scope = scopeSpans.hasScope() ? scopeSpans.getScope() : null;
                for (var span : scopeSpans.getSpansList()) {
                    rows.add(SpanConverter.toRow(span, resource, scope));
                }
            }
        }
        log.debug("Received {} spans", rows.size());
        writer.addBatch(rows).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to persist {} spans", rows.size(), ex);
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        });
    }
}
