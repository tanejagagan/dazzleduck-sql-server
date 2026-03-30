package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;

/**
 * Holds the three protobuf objects that together describe a single span.
 */
public record SpanEntry(Span span, Resource resource, InstrumentationScope scope) {}
