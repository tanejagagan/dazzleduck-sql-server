package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.otel.collector.auth.JwtServerInterceptor;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the OTLP gRPC server and signal writers.
 *
 * <p>Writers are keyed by queue ID, sourced from
 * {@code otel_collector.ingestion_task_factory_provider.ingestion_queue_table_mapping}.
 * Every request must carry the {@code x-dd-ingestion-queue} JWT claim — there is no default
 * fallback. JWT authentication is the only supported mode.
 */
public class OtelCollectorServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelCollectorServer.class);

    private final CollectorProperties props;
    private Server grpcServer;
    private Map<String, SignalWriter> writers;
    private OtelLogService logService;
    private OtelTraceService traceService;
    private OtelMetricsService metricsService;
    private OtelCollectorMetrics collectorMetrics;

    public OtelCollectorServer(CollectorProperties props) {
        this.props = props;
    }

    public void start() throws IOException {
        try {
            var handler = props.getIngestionHandler();
            var ingestionConfig = props.getIngestionConfig();

            var queueIds = props.getQueues();
            if (queueIds == null || queueIds.isEmpty()) {
                throw new IllegalStateException("otel_collector.queues must contain at least one queue name");
            }
            writers = new LinkedHashMap<>();
            for (String queueId : queueIds) {
                writers.put(queueId, new SignalWriter(queueId, handler, ingestionConfig));
            }

            setupCommonTags(props.getMeterRegistry(), props.getServiceName());
            collectorMetrics = new OtelCollectorMetrics(props.getMeterRegistry());
            writers.forEach(collectorMetrics::registerWriter);

            logService     = new OtelLogService(writers, collectorMetrics);
            traceService   = new OtelTraceService(writers, collectorMetrics);
            metricsService = new OtelMetricsService(writers, collectorMetrics);

            if (!"jwt".equals(props.getAuthentication())) {
                throw new IllegalStateException(
                        "Unsupported authentication mode: '" + props.getAuthentication() + "'. Only 'jwt' is supported.");
            }
            if (props.getSecretKey() == null || props.getSecretKey().isEmpty()) {
                throw new IllegalStateException("otel_collector.secret_key is required");
            }
            var secretKey = Validator.fromBase64String(props.getSecretKey());
            var userHashMap = new java.util.HashMap<String, byte[]>();
            props.getUsers().forEach((u, p) -> userHashMap.put(u, Validator.hash(p)));

            var builder = NettyServerBuilder
                    .forPort(props.getGrpcPort())
                    .addService(logService)
                    .addService(traceService)
                    .addService(metricsService);
            builder.intercept(new JwtServerInterceptor(secretKey, userHashMap,
                    props.getJwtExpiration(), props.getLoginUrl(), props.isVerifySignature()));

            if (props.getLoginUrl() != null) {
                log.info("JWT authentication enabled with login delegation to {}", props.getLoginUrl());
            } else {
                log.info("JWT authentication enabled for {} user(s)", userHashMap.size());
            }

            grpcServer = builder.build().start();

            log.info("OTLP gRPC server started on port {} — queues: {}",
                    props.getGrpcPort(), writers.keySet());
        } catch (Exception e) {
            close();
            if (e instanceof IOException ioe) throw ioe;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException("Server startup failed", e);
        }
    }

    private static void setupCommonTags(MeterRegistry registry, String serviceName) {
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null || hostname.isBlank()) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostname = "unknown";
            }
        }
        String containerId = System.getenv().getOrDefault("CONTAINER_ID", "unknown");
        registry.config().commonTags(
                "service.name", serviceName,
                "host.name", hostname,
                "container.id", containerId);
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    @Override
    public void close() {
        if (grpcServer != null) {
            log.info("Shutting down gRPC server...");
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(10, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        closeQuietly("logService",       logService);
        closeQuietly("traceService",     traceService);
        closeQuietly("metricsService",   metricsService);
        closeQuietly("collectorMetrics", collectorMetrics);
        if (writers != null) {
            writers.forEach((id, w) -> closeQuietly("writer[" + id + "]", w));
        }
        log.info("OtelCollectorServer stopped.");
    }

    private void closeQuietly(String name, Closeable c) {
        if (c != null) {
            try { c.close(); } catch (Exception e) { log.warn("Error closing {}", name, e); }
        }
    }
}
