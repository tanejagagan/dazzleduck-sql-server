package io.dazzleduck.sql.otel.collector;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Differential guard for {@link SpanListWriter}, which replaced Arrow's writer API for the
 * {@code events} and {@code links} columns. Like {@link MapColumnWriterTest} it asserts against
 * the replaced code's own output — kept below verbatim — so any difference in layout (a null list
 * written as empty, a null map as an empty one, a shifted element) fails here, not in a query.
 * {@link LargeBatchAlignmentTest} checks the same columns against the OTLP input instead.
 */
class SpanListWriterTest {

    @ParameterizedTest(name = "stringCopy={0}")
    @ValueSource(booleans = {true, false})
    void producesTheSameArrowAsTheWriterApiItReplaced(boolean stringCopy) {
        Random rng = new Random(42);
        List<SpanEntry> entries = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
            entries.add(new SpanEntry(LargeBatchAlignmentTest.randomSpan(rng), null, null));
        }
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot actual = VectorSchemaRoot.create(OtelTraceSchema.SCHEMA, allocator);
             VectorSchemaRoot expected = VectorSchemaRoot.create(OtelTraceSchema.SCHEMA, allocator)) {

            SpanBatchWriter.write(entries, actual, stringCopy);
            writeTheOldWay(entries, expected, stringCopy);

            for (int col : new int[]{OtelTraceSchema.COL_EVENTS, OtelTraceSchema.COL_LINKS}) {
                ListVector a = (ListVector) actual.getVector(col);
                ListVector e = (ListVector) expected.getVector(col);
                assertEquals(e.getValueCount(), a.getValueCount(), "value count, column " + col);
                assertEquals(e.getDataVector().getValueCount(), a.getDataVector().getValueCount(),
                        "element count, column " + col);
                for (int row = 0; row < entries.size(); row++) {
                    assertEquals(String.valueOf(e.getObject(row)), String.valueOf(a.getObject(row)),
                            "column " + col + ", row " + row);
                }
            }
        }
    }

    private static void writeTheOldWay(List<SpanEntry> entries, VectorSchemaRoot root, boolean stringCopy) {
        root.allocateNew();
        ListVector eventsVec = (ListVector) root.getVector(OtelTraceSchema.COL_EVENTS);
        ListVector linksVec = (ListVector) root.getVector(OtelTraceSchema.COL_LINKS);
        for (int i = 0; i < entries.size(); i++) {
            Span span = entries.get(i).span();
            writeEvents(eventsVec.getWriter(), i, span, stringCopy);
            writeLinks(linksVec.getWriter(), i, span, stringCopy);
        }
        root.setRowCount(entries.size());
    }

    // ---- Verbatim from SpanBatchWriter before SpanListWriter replaced it: the reference output. ----

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
            writeStructVarChar(sw, "trace_id", SpanBatchWriter.hexOrNull(l.getTraceId().toByteArray()));
            writeStructVarChar(sw, "span_id", SpanBatchWriter.hexOrNull(l.getSpanId().toByteArray()));
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
}
