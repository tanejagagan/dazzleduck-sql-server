package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.commons.auth.Validator;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the OTLP gRPC server.
 *
 * <p>Queues are not pre-created at startup. Each request is resolved straight through the
 * {@link IngestionHandler}, which is the single registry: it is the source of truth for which
 * queues exist ({@link IngestionHandler#getKnownQueues()}) and where each writes
 * ({@link IngestionHandler#getTargetPath(String)}), and it lazily creates / caches / evicts the
 * underlying {@code ParquetIngestionQueue} per ID. So queues added to or removed from a dynamic
 * registry (e.g. {@code DynamicDuckLakeIngestionTaskFactoryProvider}) are routable/rejected
 * without a restart. The three signal services share one {@link #flushScheduler} for time-based
 * flushes. Every request must carry the {@code x-dd-ingestion-queue} JWT claim — there is no
 * default fallback. JWT authentication is the only supported mode.
 */
public class OtelCollectorServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(OtelCollectorServer.class);

    private final CollectorProperties props;
    private Server grpcServer;
    private IngestionHandler handler;
    private ScheduledExecutorService flushScheduler;
    private OtelLogService logService;
    private OtelTraceService traceService;
    private OtelMetricsService metricsService;
    private OtelCollectorMetrics collectorMetrics;

    public OtelCollectorServer(CollectorProperties props) {
        this.props = props;
    }

    public void start() throws IOException {
        try {
            handler = props.getIngestionHandler();
            var ingestionConfig = props.getIngestionConfig();

            // No startup seeding: the handler is the single registry. Queues are created lazily on
            // first use of a known queue and evicted when the handler stops reporting them.
            // All queues share one flush scheduler; an evicted queue's stray flush is a no-op.
            setupCommonTags(props.getMeterRegistry(), props.getServiceName());
            collectorMetrics = new OtelCollectorMetrics(props.getMeterRegistry());
            flushScheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "otel-flush-scheduler");
                t.setDaemon(true);
                return t;
            });

            logService     = new OtelLogService(handler, ingestionConfig, flushScheduler, collectorMetrics);
            traceService   = new OtelTraceService(handler, ingestionConfig, flushScheduler, collectorMetrics);
            metricsService = new OtelMetricsService(handler, ingestionConfig, flushScheduler, collectorMetrics);

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

            log.info("OTLP gRPC server started on port {} — known queues: {} (writers created lazily on first use)",
                    props.getGrpcPort(), handler.getKnownQueues());
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
        // Flush and close all queues the handler owns, then stop the shared flush scheduler.
        if (handler != null) {
            try { handler.closeQueues(); } catch (Exception e) { log.warn("Error closing ingestion queues", e); }
        }
        if (flushScheduler != null) {
            flushScheduler.shutdownNow();
        }
        closeQuietly("logService",       logService);
        closeQuietly("traceService",     traceService);
        closeQuietly("metricsService",   metricsService);
        closeQuietly("collectorMetrics", collectorMetrics);
        log.info("OtelCollectorServer stopped.");
    }

    private void closeQuietly(String name, Closeable c) {
        if (c != null) {
            try { c.close(); } catch (Exception e) { log.warn("Error closing {}", name, e); }
        }
    }
}
