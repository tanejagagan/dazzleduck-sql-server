package io.dazzleduck.sql.otel.collector;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for the {@code /health} {@code batchesProcessed} field's data source: it must
 * keep counting across queue eviction, unlike the per-queue {@code batches_written}
 * {@code FunctionCounter}s, which {@link OtelCollectorMetrics#unregisterQueue} deliberately removes
 * so an evicted queue's {@code ParquetIngestionQueue} can be garbage-collected.
 */
class OtelCollectorMetricsTest {

    @Test
    void totalBatchesProcessedSurvivesQueueUnregistration() {
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());

        metrics.recordExport("q1", 5, metrics.startSample());
        assertEquals(1, metrics.getTotalBatchesProcessed());

        metrics.unregisterQueue("q1"); // simulates eviction/drain — must not reset the total
        assertEquals(1, metrics.getTotalBatchesProcessed());

        metrics.recordExport("q1", 3, metrics.startSample()); // queue recreated under the same id
        assertEquals(2, metrics.getTotalBatchesProcessed());
    }
}
