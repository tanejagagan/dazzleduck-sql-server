package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Holds the three protobuf objects that together describe a single metric.
 * One MetricEntry may expand to multiple rows (one per data point) when written.
 */
record MetricEntry(Metric metric, Resource resource, InstrumentationScope scope) {}
