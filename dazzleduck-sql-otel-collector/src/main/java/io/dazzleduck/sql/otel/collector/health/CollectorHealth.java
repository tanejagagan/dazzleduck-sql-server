package io.dazzleduck.sql.otel.collector.health;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Thread-safe holder for the collector's current {@link CollectorHealthStatus}, read by
 * {@link HealthServer} on every request.
 *
 * <p>Starts at {@code MAINTENANCE}: the server isn't ready to take traffic until
 * {@code OtelCollectorServer.start()} successfully binds the gRPC port and flips it to
 * {@code HEALTHY}.
 */
public class CollectorHealth {

    private final AtomicReference<CollectorHealthStatus> status =
            new AtomicReference<>(CollectorHealthStatus.MAINTENANCE);
    private final Instant startTime = Instant.now();
    private final IntSupplier knownQueueCountSupplier;
    private final LongSupplier batchesProcessedSupplier;

    public CollectorHealth(IntSupplier knownQueueCountSupplier, LongSupplier batchesProcessedSupplier) {
        this.knownQueueCountSupplier = knownQueueCountSupplier;
        this.batchesProcessedSupplier = batchesProcessedSupplier;
    }

    public CollectorHealthStatus getStatus() {
        return status.get();
    }

    public void transitionTo(CollectorHealthStatus newStatus) {
        status.set(newStatus);
    }

    public long uptimeSeconds() {
        return Instant.now().getEpochSecond() - startTime.getEpochSecond();
    }

    public int knownQueueCount() {
        return knownQueueCountSupplier.getAsInt();
    }

    public long batchesProcessed() {
        return batchesProcessedSupplier.getAsLong();
    }
}
