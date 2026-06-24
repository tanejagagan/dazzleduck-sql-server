package io.dazzleduck.sql.otel.collector.health;

/**
 * Lifecycle status of the collector, surfaced via {@link HealthServer}.
 *
 * <p>{@code MAINTENANCE} is entered at the start of a graceful shutdown — readiness probes should
 * fail and traffic should stop routing here, while the gRPC server itself keeps draining in-flight
 * calls for a short grace period before actually stopping ({@code DOWN}).
 */
public enum CollectorHealthStatus {
    HEALTHY,
    MAINTENANCE,
    DOWN
}
