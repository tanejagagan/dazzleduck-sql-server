package io.dazzleduck.sql.otel.collector;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code otel_collector.metrics.write_description}: a metric's OTLP description repeats on every
 * data point, so it is written only when asked for. The column itself always exists.
 */
class MetricBatchWriterTest {

    private static final Metric GAUGE = Metric.newBuilder()
            .setName("queue.depth")
            .setDescription("Batches waiting to be written")
            .setUnit("{batch}")
            .setGauge(Gauge.newBuilder()
                    .addDataPoints(NumberDataPoint.newBuilder().setTimeUnixNano(1_000_000_000L).setAsInt(3))
                    .addDataPoints(NumberDataPoint.newBuilder().setTimeUnixNano(2_000_000_000L).setAsInt(5)))
            .build();

    @Test
    void descriptionIsNullOnEveryRowWhenDisabled() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelMetricSchema.SCHEMA, allocator)) {
            MetricBatchWriter.write(List.of(new MetricEntry(GAUGE, null, null)), root, false);

            assertEquals(2, root.getRowCount());
            VarCharVector description = (VarCharVector) root.getVector(OtelMetricSchema.COL_DESCRIPTION);
            for (int row = 0; row < root.getRowCount(); row++) {
                assertTrue(description.isNull(row), "row " + row);
                assertEquals("queue.depth", text(root, OtelMetricSchema.COL_NAME, row));
                assertEquals("{batch}", text(root, OtelMetricSchema.COL_UNIT, row), "unit is still written");
            }
        }
    }

    @Test
    void descriptionIsWrittenWhenEnabled() {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelMetricSchema.SCHEMA, allocator)) {
            MetricBatchWriter.write(List.of(new MetricEntry(GAUGE, null, null)), root, true);

            assertEquals(2, root.getRowCount());
            for (int row = 0; row < root.getRowCount(); row++) {
                assertEquals("Batches waiting to be written", text(root, OtelMetricSchema.COL_DESCRIPTION, row));
            }
        }
    }

    @Test
    void configDefaultsToNotWritingDescriptions() {
        assertFalse(new CollectorConfig().getMetricsWriteDescription());
        assertFalse(new CollectorConfig().toProperties().isMetricsWriteDescription());
        assertFalse(new CollectorProperties().isMetricsWriteDescription(),
                "the programmatic default must match reference.conf");
    }

    @Test
    void configCanTurnDescriptionsOn() {
        var config = ConfigFactory.parseString("otel_collector.metrics.write_description = true")
                .withFallback(ConfigFactory.load()).resolve();
        assertTrue(new CollectorConfig(config).toProperties().isMetricsWriteDescription());
    }

    private static String text(VectorSchemaRoot root, int column, int row) {
        return new String(((VarCharVector) root.getVector(column)).get(row), StandardCharsets.UTF_8);
    }
}
