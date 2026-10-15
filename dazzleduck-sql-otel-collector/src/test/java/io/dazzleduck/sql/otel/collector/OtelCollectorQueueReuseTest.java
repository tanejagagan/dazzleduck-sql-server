package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.sampleLogRequest;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression: with the default handler (plain Parquet, {@link NOOPIngestionTaskFactoryProvider}),
 * every OTLP export used to build a new ingestion queue — and with it a writer thread that was
 * never stopped — so the thread count grew by one per request until the JVM could not create
 * threads, and no two exports ever shared a batch. One queue must serve every export, and stop on
 * shutdown.
 */
class OtelCollectorQueueReuseTest {

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
    }

    private static long liveWriterThreads(String queueId) {
        String name = "BulkIngestQueue-" + queueId + "-writer";
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().equals(name))
                .count();
    }

    @Test
    void defaultHandlerServesEveryExportFromOneQueue() throws Exception {
        Path outputRoot = Files.createTempDirectory("otel-queue-reuse");
        Files.createDirectories(outputRoot.resolve("logs"));
        IngestionHandler handler = new NOOPIngestionTaskFactoryProvider(outputRoot.toString()).getIngestionHandler();

        var props = new CollectorProperties();
        props.setShutdownGracePeriod(Duration.ZERO);
        props.setGrpcPort(findFreePort());
        props.setIngestionHandler(handler);
        props.setIngestionConfig(smallBucketConfig());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);

        long before = liveWriterThreads("logs");
        OtelCollectorServer server = new OtelCollectorServer(props);
        server.start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();
        try {
            var stub = LogsServiceGrpc.newBlockingStub(channel).withInterceptors(
                    MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim("logs"))));
            for (int i = 0; i < 20; i++) {
                stub.export(sampleLogRequest());
            }

            assertEquals(1, handler.getQueueStats().size(), "one queue for every export");
            assertEquals(before + 1, liveWriterThreads("logs"), "one writer thread, not one per export");
        } finally {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.close();
        }
        assertEquals(before, liveWriterThreads("logs"), "closing the collector stops the queue's writer");
    }
}
