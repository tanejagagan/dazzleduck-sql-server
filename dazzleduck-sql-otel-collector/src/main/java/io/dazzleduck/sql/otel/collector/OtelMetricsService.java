package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * gRPC service that receives OTLP metric exports and writes them to Parquet.
 */
public class OtelMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelMetricsService.class);

    private final SignalWriter writer;

    public OtelMetricsService(SignalWriter writer) {
        this.writer = writer;
    }

    @Override
    public void export(ExportMetricsServiceRequest request,
                       StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        List<JavaRow> rows = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            var resource = resourceMetrics.hasResource() ? resourceMetrics.getResource() : null;
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                var scope = scopeMetrics.hasScope() ? scopeMetrics.getScope() : null;
                for (var metric : scopeMetrics.getMetricsList()) {
                    rows.addAll(MetricConverter.toRows(metric, resource, scope));
                }
            }
        }
        log.debug("Received {} metric data points", rows.size());
        writer.addBatch(rows).whenComplete((v, ex) -> {
            if (ex != null) {
                log.error("Failed to persist {} metric data points", rows.size(), ex);
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        });
    }
}
