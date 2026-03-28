package io.dazzleduck.sql.otel.collector;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
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
 * gRPC service that receives OTLP metric exports and writes them to Parquet.
 */
public class OtelMetricsService extends MetricsServiceGrpc.MetricsServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OtelMetricsService.class);

    private final SignalWriter writer;
    private final Path tempDir;
    private final OtelCollectorMetrics metrics;

    public OtelMetricsService(SignalWriter writer, OtelCollectorMetrics metrics) throws IOException {
        this.writer = writer;
        this.metrics = metrics;
        this.tempDir = Files.createTempDirectory("otel-metrics-arrow-");
    }

    @Override
    public void export(ExportMetricsServiceRequest request,
                       StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        List<MetricEntry> entries = new ArrayList<>();
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            var resource = resourceMetrics.hasResource() ? resourceMetrics.getResource() : null;
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                var scope = scopeMetrics.hasScope() ? scopeMetrics.getScope() : null;
                for (var metric : scopeMetrics.getMetricsList()) {
                    entries.add(new MetricEntry(metric, resource, scope));
                }
            }
        }
        log.debug("Received {} metrics", entries.size());
        var sample = metrics.startSample();

        try {
            Path arrowFile = writeArrowFile(entries);
            writer.addBatch(arrowFile).whenComplete((v, ex) -> {
                if (ex != null) {
                    metrics.recordMetricError(sample);
                    log.error("Failed to persist {} metrics", entries.size(), ex);
                    responseObserver.onError(ex);
                } else {
                    metrics.recordMetricExport(entries.size(), sample);
                    responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            });
        } catch (IOException e) {
            metrics.recordMetricError(sample);
            log.error("Failed to write Arrow file for {} metrics", entries.size(), e);
            responseObserver.onError(e);
        }
    }

    private Path writeArrowFile(List<MetricEntry> entries) throws IOException {
        Path tempFile = tempDir.resolve("batch_" + UUID.randomUUID() + ".arrow");
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelMetricSchema.SCHEMA, allocator)) {
            MetricBatchWriter.write(entries, root);
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
