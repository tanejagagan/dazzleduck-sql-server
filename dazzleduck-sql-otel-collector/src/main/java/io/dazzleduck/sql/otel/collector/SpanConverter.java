package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.common.types.JavaRow;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Flattens an OTLP Span (with Resource and InstrumentationScope context)
 * into a single JavaRow matching OtelTraceSchema.
 */
public class SpanConverter {

    private static final HexFormat HEX = HexFormat.of();

    public static JavaRow toRow(Span span, Resource resource, InstrumentationScope scope) {
        Object[] values = new Object[16];

        byte[] traceId = span.getTraceId().toByteArray();
        values[OtelTraceSchema.COL_TRACE_ID] = traceId.length > 0 ? HEX.formatHex(traceId) : null;

        byte[] spanId = span.getSpanId().toByteArray();
        values[OtelTraceSchema.COL_SPAN_ID] = spanId.length > 0 ? HEX.formatHex(spanId) : null;

        byte[] parentSpanId = span.getParentSpanId().toByteArray();
        values[OtelTraceSchema.COL_PARENT_SPAN_ID] = parentSpanId.length > 0 ? HEX.formatHex(parentSpanId) : null;

        values[OtelTraceSchema.COL_NAME] = LogRecordConverter.emptyToNull(span.getName());

        String kind = span.getKind().name();
        if (kind.startsWith("SPAN_KIND_")) kind = kind.substring("SPAN_KIND_".length());
        values[OtelTraceSchema.COL_KIND] = kind;

        long startNanos = span.getStartTimeUnixNano();
        long endNanos = span.getEndTimeUnixNano();
        long startMs = startNanos > 0 ? startNanos / 1_000_000L : 0L;
        long endMs = endNanos > 0 ? endNanos / 1_000_000L : 0L;
        values[OtelTraceSchema.COL_START_TIME_MS] = startNanos > 0 ? startMs : null;
        values[OtelTraceSchema.COL_END_TIME_MS] = endNanos > 0 ? endMs : null;
        values[OtelTraceSchema.COL_DURATION_MS] = (startNanos > 0 && endNanos > 0) ? (endMs - startMs) : null;

        Status status = span.getStatus();
        String statusCode = status.getCode().name();
        if (statusCode.startsWith("STATUS_CODE_")) statusCode = statusCode.substring("STATUS_CODE_".length());
        values[OtelTraceSchema.COL_STATUS_CODE] = statusCode;
        values[OtelTraceSchema.COL_STATUS_MESSAGE] = LogRecordConverter.emptyToNull(status.getMessage());

        values[OtelTraceSchema.COL_ATTRIBUTES] = LogRecordConverter.kvListToMap(span.getAttributesList());
        values[OtelTraceSchema.COL_RESOURCE_ATTRIBUTES] = resource != null
                ? LogRecordConverter.kvListToMap(resource.getAttributesList())
                : new LinkedHashMap<>();

        values[OtelTraceSchema.COL_SCOPE_NAME] = scope != null ? LogRecordConverter.emptyToNull(scope.getName()) : null;
        values[OtelTraceSchema.COL_SCOPE_VERSION] = scope != null ? LogRecordConverter.emptyToNull(scope.getVersion()) : null;

        values[OtelTraceSchema.COL_EVENTS] = eventsToList(span);
        values[OtelTraceSchema.COL_LINKS] = linksToList(span);

        return new JavaRow(values);
    }

    private static List<Object[]> eventsToList(Span span) {
        if (span.getEventsCount() == 0) return null;
        List<Object[]> result = new ArrayList<>();
        for (var e : span.getEventsList()) {
            result.add(new Object[]{
                    LogRecordConverter.emptyToNull(e.getName()),
                    e.getTimeUnixNano() > 0 ? e.getTimeUnixNano() / 1_000_000L : null,
                    e.getAttributesCount() > 0 ? LogRecordConverter.kvListToMap(e.getAttributesList()) : null
            });
        }
        return result;
    }

    private static List<Object[]> linksToList(Span span) {
        if (span.getLinksCount() == 0) return null;
        List<Object[]> result = new ArrayList<>();
        for (var l : span.getLinksList()) {
            byte[] tid = l.getTraceId().toByteArray();
            byte[] sid = l.getSpanId().toByteArray();
            result.add(new Object[]{
                    tid.length > 0 ? HEX.formatHex(tid) : null,
                    sid.length > 0 ? HEX.formatHex(sid) : null,
                    l.getAttributesCount() > 0 ? LogRecordConverter.kvListToMap(l.getAttributesList()) : null
            });
        }
        return result;
    }

}
