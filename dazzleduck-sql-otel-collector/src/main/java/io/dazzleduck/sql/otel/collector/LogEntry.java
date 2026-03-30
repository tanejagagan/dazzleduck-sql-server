package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Holds the three protobuf objects that together describe a single log record.
 */
record LogEntry(LogRecord record, Resource resource, InstrumentationScope scope) {}
