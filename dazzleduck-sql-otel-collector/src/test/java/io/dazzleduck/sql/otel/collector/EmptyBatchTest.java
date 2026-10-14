package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An export can carry no records at all, and nothing upstream filters it out, so a batch of zero
 * rows reaches the buffered writers: every buffer is zero-length (one zero offset, an empty
 * bitmap). It must load, serialize to the Arrow staging file, read back, and leave the root
 * usable for the next batch.
 */
class EmptyBatchTest {

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
    }

    private static <E> void emptyThenFull(Schema schema, BiConsumer<List<E>, VectorSchemaRoot> writer, List<E> full) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            writer.accept(List.of(), root);
            assertEquals(0, root.getRowCount());
            assertEquals(0, roundTrip(root, allocator), "the empty batch survives the Arrow IPC write");

            writer.accept(full, root);
            assertEquals(full.size(), roundTrip(root, allocator), "the root is reusable after an empty batch");
        }
    }

    /** Writes {@code root} as the staging file does (Arrow IPC stream) and returns the rows read back. */
    private static int roundTrip(VectorSchemaRoot root, BufferAllocator allocator) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            int rows = 0;
            while (reader.loadNextBatch()) rows += reader.getVectorSchemaRoot().getRowCount();
            return rows;
        }
    }

    @Test
    void logs() throws Exception {
        Random rng = new Random(1);
        List<LogEntry> full = List.of(new LogEntry(
                io.opentelemetry.proto.logs.v1.LogRecord.newBuilder()
                        .addAllAttributes(LargeBatchAlignmentTest.randomAttributes(rng, 20)).build(), null, null));
        emptyThenFull(OtelLogSchema.SCHEMA, LogRecordBatchWriter::write, full);
    }

    @Test
    void spans() throws Exception {
        Random rng = new Random(2);
        List<SpanEntry> full = List.of(new SpanEntry(LargeBatchAlignmentTest.randomSpan(rng), null, null),
                new SpanEntry(LargeBatchAlignmentTest.randomSpan(rng), null, null));
        emptyThenFull(OtelTraceSchema.SCHEMA, SpanBatchWriter::write, full);
    }

    @Test
    void metrics() throws Exception {
        var gauge = io.opentelemetry.proto.metrics.v1.Metric.newBuilder().setName("m")
                .setGauge(io.opentelemetry.proto.metrics.v1.Gauge.newBuilder()
                        .addDataPoints(io.opentelemetry.proto.metrics.v1.NumberDataPoint.newBuilder().setAsInt(1)))
                .build();
        // A metric with no data points yields no rows either, so it rides along in the empty batch.
        var noPoints = io.opentelemetry.proto.metrics.v1.Metric.newBuilder().setName("none")
                .setGauge(io.opentelemetry.proto.metrics.v1.Gauge.getDefaultInstance()).build();
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelMetricSchema.SCHEMA, allocator)) {
            MetricBatchWriter.write(List.of(new MetricEntry(noPoints, null, null)), root, false);
            assertEquals(0, root.getRowCount());
            assertEquals(0, roundTrip(root, allocator));
            MetricBatchWriter.write(List.of(new MetricEntry(gauge, null, null)), root, false);
            assertEquals(1, roundTrip(root, allocator));
        }
    }

    /** End to end: empty exports of every signal are accepted by a running collector. */
    @Test
    void emptyExportsAreAccepted() throws Exception {
        Path outputRoot = Files.createTempDirectory("otel-empty-export");
        for (String q : new String[]{"logs", "traces", "metrics"}) Files.createDirectories(outputRoot.resolve(q));
        var props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO);
        props.setGrpcPort(findFreePort());
        props.setIngestionHandler(new NOOPIngestionTaskFactoryProvider(outputRoot.toString()).getIngestionHandler());
        props.setIngestionConfig(smallBucketConfig());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        OtelCollectorServer server = new OtelCollectorServer(props);
        server.start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
        try {
            LogsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("logs"))
                    .export(ExportLogsServiceRequest.newBuilder()
                            .addResourceLogs(ResourceLogs.newBuilder().addScopeLogs(ScopeLogs.getDefaultInstance())).build());
            LogsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("logs"))
                    .export(ExportLogsServiceRequest.getDefaultInstance());
            TraceServiceGrpc.newBlockingStub(channel).withInterceptors(auth("traces"))
                    .export(ExportTraceServiceRequest.newBuilder()
                            .addResourceSpans(ResourceSpans.newBuilder().addScopeSpans(ScopeSpans.getDefaultInstance())).build());
            MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("metrics"))
                    .export(ExportMetricsServiceRequest.newBuilder()
                            .addResourceMetrics(ResourceMetrics.newBuilder().addScopeMetrics(ScopeMetrics.getDefaultInstance())).build());
        } finally {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.close();
        }
    }

    private static io.grpc.ClientInterceptor auth(String queue) {
        return MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim(queue)));
    }
}
