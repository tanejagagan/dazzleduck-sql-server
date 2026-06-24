package io.dazzleduck.sql.otel.collector.health;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CollectorHealthTest {

    @Test
    void startsInMaintenanceAndTransitionsAcrossStates() {
        CollectorHealth health = new CollectorHealth(() -> 3, () -> 0);

        assertEquals(CollectorHealthStatus.MAINTENANCE, health.getStatus());

        health.transitionTo(CollectorHealthStatus.HEALTHY);
        assertEquals(CollectorHealthStatus.HEALTHY, health.getStatus());

        health.transitionTo(CollectorHealthStatus.MAINTENANCE);
        assertEquals(CollectorHealthStatus.MAINTENANCE, health.getStatus());

        health.transitionTo(CollectorHealthStatus.DOWN);
        assertEquals(CollectorHealthStatus.DOWN, health.getStatus());
    }

    @Test
    void reportsKnownQueueCountFromSupplier() {
        CollectorHealth health = new CollectorHealth(() -> 7, () -> 0);
        assertEquals(7, health.knownQueueCount());
    }

    @Test
    void reportsBatchesProcessedFromSupplier() {
        CollectorHealth health = new CollectorHealth(() -> 0, () -> 42);
        assertEquals(42, health.batchesProcessed());
    }

    @Test
    void uptimeSecondsIsNonNegative() {
        CollectorHealth health = new CollectorHealth(() -> 0, () -> 0);
        assertTrue(health.uptimeSeconds() >= 0);
    }
}
