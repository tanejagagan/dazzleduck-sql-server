package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * Writes a batch of OTLP spans directly into an Arrow VectorSchemaRoot,
 * bypassing the JavaRow intermediate representation.
 */
public class SpanBatchWriter {

    private static final HexFormat HEX = HexFormat.of();

    public static void write(List<SpanEntry> entries, VectorSchemaRoot root) {
        write(entries, root, true);
    }

    /**
     * @param stringCopy also write every non-string attribute value, as text, to the string
     *                   attribute maps — the span's, its resource's, and its events' and links'
     */
    public static void write(List<SpanEntry> entries, VectorSchemaRoot root, boolean stringCopy) {
        root.allocateNew();

        VarCharVector traceIdVec        = (VarCharVector)        root.getVector(OtelTraceSchema.COL_TRACE_ID);
        VarCharVector spanIdVec         = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SPAN_ID);
        VarCharVector parentSpanIdVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_PARENT_SPAN_ID);
        VarCharVector nameVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_NAME);
        VarCharVector kindVec           = (VarCharVector)        root.getVector(OtelTraceSchema.COL_KIND);
        TimeStampMilliVector startVec   = (TimeStampMilliVector) root.getVector(OtelTraceSchema.COL_START_TIME_MS);
        TimeStampMilliVector endVec     = (TimeStampMilliVector) root.getVector(OtelTraceSchema.COL_END_TIME_MS);
        BigIntVector durationVec        = (BigIntVector)         root.getVector(OtelTraceSchema.COL_DURATION_MS);
        VarCharVector statusCodeVec     = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_CODE);
        VarCharVector statusMsgVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_STATUS_MESSAGE);
        VarCharVector scopeNameVec      = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_NAME);
        VarCharVector scopeVersionVec   = (VarCharVector)        root.getVector(OtelTraceSchema.COL_SCOPE_VERSION);
        ListVector eventsVec            = (ListVector)           root.getVector(OtelTraceSchema.COL_EVENTS);
        ListVector linksVec             = (ListVector)           root.getVector(OtelTraceSchema.COL_LINKS);

        AttributeMapsWriter attrWriter = AttributeMapsWriter.of(root,
                OtelTraceSchema.COL_ATTRIBUTES, OtelTraceSchema.COL_TYPED_ATTRIBUTES, stringCopy);
        AttributeMapsWriter resAttrWriter = AttributeMapsWriter.of(root,
                OtelTraceSchema.COL_RESOURCE_ATTRIBUTES, OtelTraceSchema.COL_TYPED_RESOURCE_ATTRIBUTES, stringCopy);

        for (int i = 0; i < entries.size(); i++) {
            SpanEntry entry = entries.get(i);
            Span span = entry.span();
            Resource resource = entry.resource();
            InstrumentationScope scope = entry.scope();

            writeVarChar(traceIdVec, i, hexOrNull(span.getTraceId().toByteArray()));
            writeVarChar(spanIdVec, i, hexOrNull(span.getSpanId().toByteArray()));
            writeVarChar(parentSpanIdVec, i, hexOrNull(span.getParentSpanId().toByteArray()));
            writeVarChar(nameVec, i, LogRecordConverter.emptyToNull(span.getName()));

            String kind = span.getKind().name();
            if (kind.startsWith("SPAN_KIND_")) kind = kind.substring("SPAN_KIND_".length());
            writeVarChar(kindVec, i, kind);

            long startNanos = span.getStartTimeUnixNano();
            long endNanos   = span.getEndTimeUnixNano();
            long startMs = startNanos > 0 ? startNanos / 1_000_000L : 0L;
            long endMs   = endNanos   > 0 ? endNanos   / 1_000_000L : 0L;
            if (startNanos > 0) startVec.setSafe(i, startMs); else startVec.setNull(i);
            if (endNanos   > 0) endVec.setSafe(i, endMs);     else endVec.setNull(i);
            if (startNanos > 0 && endNanos > 0) durationVec.setSafe(i, endMs - startMs);
            else durationVec.setNull(i);

            Status status = span.getStatus();
            String statusCode = status.getCode().name();
            if (statusCode.startsWith("STATUS_CODE_")) statusCode = statusCode.substring("STATUS_CODE_".length());
            writeVarChar(statusCodeVec, i, statusCode);
            writeVarChar(statusMsgVec, i, LogRecordConverter.emptyToNull(status.getMessage()));

            attrWriter.write(i, span.getAttributesList());
            resAttrWriter.write(i, resource != null ? resource.getAttributesList() : List.of());

            if (scope != null) {
                writeVarChar(scopeNameVec, i, LogRecordConverter.emptyToNull(scope.getName()));
                writeVarChar(scopeVersionVec, i, LogRecordConverter.emptyToNull(scope.getVersion()));
            } else {
                scopeNameVec.setNull(i);
                scopeVersionVec.setNull(i);
            }

            writeEvents(eventsVec.getWriter(), i, span, stringCopy);
            writeLinks(linksVec.getWriter(), i, span, stringCopy);
        }

        attrWriter.finish(entries.size());
        resAttrWriter.finish(entries.size());
        root.setRowCount(entries.size());
    }

    private static void writeEvents(BaseWriter.ListWriter writer, int index, Span span, boolean stringCopy) {
        writer.setPosition(index);
        if (span.getEventsCount() == 0) {
            writer.writeNull();
            return;
        }
        writer.startList();
        for (Span.Event e : span.getEventsList()) {
            BaseWriter.StructWriter sw = writer.struct();
            sw.start();
            writeStructVarChar(sw, "name", LogRecordConverter.emptyToNull(e.getName()));
            if (e.getTimeUnixNano() > 0) {
                sw.bigInt("time_ms").writeBigInt(e.getTimeUnixNano() / 1_000_000L);
            } else {
                sw.bigInt("time_ms").writeNull();
            }
            writeStructMaps(sw, e.getAttributesList(), stringCopy);
            sw.end();
        }
        writer.endList();
    }

    private static void writeLinks(BaseWriter.ListWriter writer, int index, Span span, boolean stringCopy) {
        writer.setPosition(index);
        if (span.getLinksCount() == 0) {
            writer.writeNull();
            return;
        }
        writer.startList();
        for (Span.Link l : span.getLinksList()) {
            BaseWriter.StructWriter sw = writer.struct();
            sw.start();
            writeStructVarChar(sw, "trace_id", hexOrNull(l.getTraceId().toByteArray()));
            writeStructVarChar(sw, "span_id", hexOrNull(l.getSpanId().toByteArray()));
            writeStructMaps(sw, l.getAttributesList(), stringCopy);
            sw.end();
        }
        writer.endList();
    }

    private static void writeStructVarChar(BaseWriter.StructWriter sw, String name, String value) {
        if (value == null) {
            sw.varChar(name).writeNull();
        } else {
            sw.varChar(name).writeVarChar(value);
        }
    }

    /**
     * Writes an event's or link's {@code attributes} map and its typed companions, routed as
     * {@link AttributeMapsWriter} does. No attributes at all leaves all six maps null.
     */
    private static void writeStructMaps(BaseWriter.StructWriter sw, List<KeyValue> kvList, boolean stringCopy) {
        for (AttributeType type : AttributeType.values()) {
            BaseWriter.MapWriter mw = sw.map("attributes" + type.suffix(), false);
            if (kvList.isEmpty()) {
                mw.writeNull();
                continue;
            }
            mw.startMap();
            for (KeyValue kv : kvList) {
                writeStructEntry(mw, type, kv.getKey(), kv.getValue(), stringCopy);
            }
            mw.endMap();
        }
    }

    /** Writes {@code value} into the {@code type} map if that is where it belongs. */
    private static void writeStructEntry(BaseWriter.MapWriter mw, AttributeType type, String key, AnyValue value,
                                         boolean stringCopy) {
        AnyValue.ValueCase kind = value.getValueCase();
        switch (type) {
            case STRING -> {
                if (stringCopy || kind == AnyValue.ValueCase.STRING_VALUE || kind == AnyValue.ValueCase.VALUE_NOT_SET) {
                    OtelSchemaFields.writeEntry(mw, key, LogRecordConverter.anyValueToString(value));
                }
            }
            case INT -> {
                if (kind == AnyValue.ValueCase.INT_VALUE) {
                    startEntry(mw, key).bigInt().writeBigInt(value.getIntValue());
                    mw.endEntry();
                }
            }
            case DOUBLE -> {
                if (kind == AnyValue.ValueCase.DOUBLE_VALUE) {
                    startEntry(mw, key).float8().writeFloat8(value.getDoubleValue());
                    mw.endEntry();
                }
            }
            case BOOL -> {
                if (kind == AnyValue.ValueCase.BOOL_VALUE) {
                    startEntry(mw, key).bit().writeBit(value.getBoolValue() ? 1 : 0);
                    mw.endEntry();
                }
            }
            case BYTES -> {
                if (kind == AnyValue.ValueCase.BYTES_VALUE) {
                    startEntry(mw, key).varBinary().writeVarBinary(value.getBytesValue().toByteArray());
                    mw.endEntry();
                }
            }
            case JSON -> {
                if (kind == AnyValue.ValueCase.ARRAY_VALUE || kind == AnyValue.ValueCase.KVLIST_VALUE) {
                    startEntry(mw, key).varChar().writeVarChar(LogRecordConverter.anyValueToJson(value));
                    mw.endEntry();
                }
            }
        }
    }

    /** Starts an entry and writes its key; returns the writer for its value. */
    private static BaseWriter.ListWriter startEntry(BaseWriter.MapWriter mw, String key) {
        mw.startEntry();
        ((BaseWriter.ListWriter) mw.key()).varChar().writeVarChar(key);
        return (BaseWriter.ListWriter) mw.value();
    }

    private static void writeVarChar(VarCharVector vec, int index, String value) {
        if (value == null) {
            vec.setNull(index);
        } else {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String hexOrNull(byte[] bytes) {
        return bytes.length > 0 ? HEX.formatHex(bytes) : null;
    }
}
