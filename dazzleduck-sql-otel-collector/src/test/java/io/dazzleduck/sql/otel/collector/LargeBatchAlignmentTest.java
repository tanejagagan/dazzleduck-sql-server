package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.Span;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Row alignment of every attribute map in batches large enough that each of the writers' heap
 * buffers grows many times over (20,000 rows, ~600,000 map entries, several MB of keys and values).
 *
 * <p>The attribute writers assemble whole batches by hand — offsets, validity bitmaps and value
 * buffers for six maps per attribute set, nested inside span events and links — and hand them to
 * Arrow in one copy. Their failure mode is not an exception but data shifted between rows, the
 * kind that shows only past a buffer boundary or after many rows. So the expectation here is built
 * from the OTLP input alone, with none of the writer code, and checked for EVERY row of every map:
 * logs, spans (and their events and links) and metrics, with the string copy on and off, and for
 * two consecutive batches through the same root so no state carries over between batches.
 */
class LargeBatchAlignmentTest {

    private static final int ROWS = 20_000;
    private static final HexFormat HEX = HexFormat.of();
    private static final String[] KEY_WORDS = {"http.status_code", "service.name", "k", "db.statement",
            "ключ", "名前", "a.very.long.attribute.key.that.crosses.many.bytes.in.the.key.buffer"};

    // ---------------------------------------------------------------- fixture

    /** 0..max attributes of every OTLP value kind, including an unset value. */
    static List<KeyValue> randomAttributes(Random rng, int max) {
        int count = rng.nextInt(max + 1);
        List<KeyValue> attrs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = KEY_WORDS[rng.nextInt(KEY_WORDS.length)] + "." + i;
            attrs.add(KeyValue.newBuilder().setKey(key).setValue(randomValue(rng, 0)).build());
        }
        return attrs;
    }

    private static AnyValue randomValue(Random rng, int depth) {
        AnyValue.Builder v = AnyValue.newBuilder();
        switch (rng.nextInt(depth > 1 ? 6 : 8)) {
            case 0 -> v.setStringValue("value-" + rng.nextInt(1000) + (rng.nextBoolean() ? "-ünïcødé" : ""));
            case 1 -> v.setIntValue(rng.nextLong());
            case 2 -> v.setDoubleValue(rng.nextGaussian() * 1e6);
            case 3 -> v.setBoolValue(rng.nextBoolean());
            case 4 -> {
                byte[] b = new byte[rng.nextInt(40)];
                rng.nextBytes(b);
                v.setBytesValue(ByteString.copyFrom(b));
            }
            case 5 -> { /* value not set */ }
            case 6 -> {
                ArrayValue.Builder a = ArrayValue.newBuilder();
                for (int i = rng.nextInt(4); i > 0; i--) a.addValues(randomValue(rng, depth + 1));
                v.setArrayValue(a);
            }
            default -> {
                KeyValueList.Builder kv = KeyValueList.newBuilder();
                for (int i = rng.nextInt(4); i > 0; i--) {
                    kv.addValues(KeyValue.newBuilder().setKey("n" + i).setValue(randomValue(rng, depth + 1)));
                }
                v.setKvlistValue(kv);
            }
        }
        return v.build();
    }

    private static Resource randomResource(Random rng) {
        return rng.nextInt(10) == 0 ? null : Resource.newBuilder().addAllAttributes(randomAttributes(rng, 20)).build();
    }

    // ---------------------------------------------------------------- expectation (no writer code)

    /** The entries {@code type}'s map must hold for {@code kvList}, rendered as {@code key=value}. */
    static List<String> expected(List<KeyValue> kvList, AttributeType type, boolean stringCopy) {
        List<String> out = new ArrayList<>();
        for (KeyValue kv : kvList) {
            AnyValue v = kv.getValue();
            String key = kv.getKey();
            AttributeType kind = switch (v.getValueCase()) {
                case STRING_VALUE, VALUE_NOT_SET -> AttributeType.STRING;
                case INT_VALUE -> AttributeType.INT;
                case DOUBLE_VALUE -> AttributeType.DOUBLE;
                case BOOL_VALUE -> AttributeType.BOOL;
                case BYTES_VALUE -> AttributeType.BYTES;
                default -> AttributeType.JSON;
            };
            if (type == kind) {
                out.add(key + "=" + switch (kind) {
                    case STRING -> v.getStringValue();   // "" when unset
                    case INT -> String.valueOf(v.getIntValue());
                    case DOUBLE -> String.valueOf(v.getDoubleValue());
                    case BOOL -> String.valueOf(v.getBoolValue());
                    case BYTES -> HEX.formatHex(v.getBytesValue().toByteArray());
                    case JSON -> LogRecordConverter.anyValueToJson(v);
                });
            } else if (type == AttributeType.STRING && stringCopy) {
                out.add(key + "=" + LogRecordConverter.anyValueToString(v));
            }
        }
        return out;
    }

    // ---------------------------------------------------------------- actual

    /** Row {@code index} of a map vector as {@code key=value} entries; null for a null map. */
    static List<String> actual(MapVector map, int index) {
        if (map.isNull(index)) return null;
        List<String> out = new ArrayList<>();
        for (Object o : (List<?>) map.getObject(index)) {
            Map<?, ?> entry = (Map<?, ?>) o;
            Object value = entry.get("value");
            String rendered = value instanceof byte[] b ? HEX.formatHex(b)
                    : value instanceof Text t ? t.toString()
                    : String.valueOf(value);
            out.add(entry.get("key") + "=" + rendered);
        }
        return out;
    }

    /** Checks one attribute set (its six maps) at {@code index}; {@code kvList} null means a null row. */
    private static void assertAttributeSet(List<MapVector> maps, int index, List<KeyValue> kvList,
                                           boolean stringCopy, String where) {
        for (AttributeType type : AttributeType.values()) {
            List<String> want = kvList == null ? null : expected(kvList, type, stringCopy);
            assertEquals(want, actual(maps.get(type.ordinal()), index), where + " " + type);
        }
    }

    /** The six maps of a top-level attribute set, in {@link AttributeType} order. */
    private static List<MapVector> maps(VectorSchemaRoot root, int stringColumn, int typedStart) {
        List<MapVector> maps = new ArrayList<>();
        maps.add((MapVector) root.getVector(stringColumn));
        for (int i = 0; i < AttributeType.TYPED.size(); i++) maps.add((MapVector) root.getVector(typedStart + i));
        return maps;
    }

    /** The six maps inside an event or link struct, in {@link AttributeType} order. */
    private static List<MapVector> maps(StructVector struct) {
        List<MapVector> maps = new ArrayList<>();
        for (AttributeType type : AttributeType.values()) {
            maps.add(struct.getChild("attributes" + type.suffix(), MapVector.class));
        }
        return maps;
    }

    // ---------------------------------------------------------------- tests

    @ParameterizedTest(name = "stringCopy={0}")
    @ValueSource(booleans = {true, false})
    void logs(boolean stringCopy) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelLogSchema.SCHEMA, allocator)) {
            for (int batch = 0; batch < 2; batch++) {
                Random rng = new Random(1000 + batch);
                List<LogEntry> entries = new ArrayList<>(ROWS);
                for (int i = 0; i < ROWS; i++) {
                    LogRecord record = LogRecord.newBuilder().setTimeUnixNano(1_000_000L * (i + 1))
                            .addAllAttributes(randomAttributes(rng, 60)).build();
                    entries.add(new LogEntry(record, randomResource(rng), null));
                }
                LogRecordBatchWriter.write(entries, root, stringCopy);

                assertEquals(ROWS, root.getRowCount());
                List<MapVector> attrs = maps(root, OtelLogSchema.COL_ATTRIBUTES, OtelLogSchema.COL_TYPED_ATTRIBUTES);
                List<MapVector> res = maps(root, OtelLogSchema.COL_RESOURCE_ATTRIBUTES, OtelLogSchema.COL_TYPED_RESOURCE_ATTRIBUTES);
                for (int row = 0; row < ROWS; row++) {
                    LogEntry e = entries.get(row);
                    assertAttributeSet(attrs, row, e.record().getAttributesList(), stringCopy, "batch " + batch + " row " + row);
                    assertAttributeSet(res, row, e.resource() == null ? List.of() : e.resource().getAttributesList(),
                            stringCopy, "batch " + batch + " resource row " + row);
                }
            }
        }
    }

    @ParameterizedTest(name = "stringCopy={0}")
    @ValueSource(booleans = {true, false})
    void spansWithEventsAndLinks(boolean stringCopy) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelTraceSchema.SCHEMA, allocator)) {
            for (int batch = 0; batch < 2; batch++) {
                Random rng = new Random(2000 + batch);
                List<SpanEntry> entries = new ArrayList<>(ROWS);
                for (int i = 0; i < ROWS; i++) entries.add(new SpanEntry(randomSpan(rng), randomResource(rng), null));
                SpanBatchWriter.write(entries, root, stringCopy);

                assertEquals(ROWS, root.getRowCount());
                List<MapVector> attrs = maps(root, OtelTraceSchema.COL_ATTRIBUTES, OtelTraceSchema.COL_TYPED_ATTRIBUTES);
                ListVector events = (ListVector) root.getVector(OtelTraceSchema.COL_EVENTS);
                ListVector links = (ListVector) root.getVector(OtelTraceSchema.COL_LINKS);
                StructVector event = (StructVector) events.getDataVector();
                StructVector link = (StructVector) links.getDataVector();
                List<MapVector> eventAttrs = maps(event);
                List<MapVector> linkAttrs = maps(link);
                int eventsSeen = 0;
                int linksSeen = 0;
                for (int row = 0; row < ROWS; row++) {
                    Span span = entries.get(row).span();
                    String where = "batch " + batch + " span " + row;
                    assertAttributeSet(attrs, row, span.getAttributesList(), stringCopy, where);

                    assertEquals(span.getEventsCount() == 0, events.isNull(row), where + " events null");
                    assertEquals(eventsSeen, events.getElementStartIndex(row), where + " events start");
                    for (Span.Event e : span.getEventsList()) {
                        String at = where + " event " + (eventsSeen - events.getElementStartIndex(row));
                        assertEquals(e.getName().isEmpty() ? null : e.getName(),
                                text(event.getChild("name", VarCharVector.class), eventsSeen), at + " name");
                        BigIntVector time = event.getChild("time_ms", BigIntVector.class);
                        assertEquals(e.getTimeUnixNano() > 0 ? e.getTimeUnixNano() / 1_000_000L : null,
                                time.isNull(eventsSeen) ? null : time.get(eventsSeen), at + " time_ms");
                        assertAttributeSet(eventAttrs, eventsSeen,
                                e.getAttributesCount() == 0 ? null : e.getAttributesList(), stringCopy, at);
                        eventsSeen++;
                    }
                    assertEquals(eventsSeen, events.getElementEndIndex(row), where + " events end");

                    assertEquals(span.getLinksCount() == 0, links.isNull(row), where + " links null");
                    for (Span.Link l : span.getLinksList()) {
                        String at = where + " link " + (linksSeen - links.getElementStartIndex(row));
                        assertEquals(SpanBatchWriter.hexOrNull(l.getTraceId().toByteArray()),
                                text(link.getChild("trace_id", VarCharVector.class), linksSeen), at + " trace_id");
                        assertEquals(SpanBatchWriter.hexOrNull(l.getSpanId().toByteArray()),
                                text(link.getChild("span_id", VarCharVector.class), linksSeen), at + " span_id");
                        assertAttributeSet(linkAttrs, linksSeen,
                                l.getAttributesCount() == 0 ? null : l.getAttributesList(), stringCopy, at);
                        linksSeen++;
                    }
                    assertEquals(linksSeen, links.getElementEndIndex(row), where + " links end");
                }
                assertEquals(eventsSeen, event.getValueCount());
                assertEquals(linksSeen, link.getValueCount());
                assertTrue(eventsSeen > ROWS, "fixture should produce more events than spans");
            }
        }
    }

    @ParameterizedTest(name = "stringCopy={0}")
    @ValueSource(booleans = {true, false})
    void metricDataPoints(boolean stringCopy) {
        try (BufferAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(OtelMetricSchema.SCHEMA, allocator)) {
            for (int batch = 0; batch < 2; batch++) {
                Random rng = new Random(3000 + batch);
                // Many metrics, each with a varying number of data points: rows are data points, so the
                // attribute rows advance inside a metric, not once per entry.
                List<MetricEntry> entries = new ArrayList<>();
                List<List<KeyValue>> pointAttrs = new ArrayList<>();
                List<Resource> pointResources = new ArrayList<>();
                while (pointAttrs.size() < ROWS) {
                    Gauge.Builder gauge = Gauge.newBuilder();
                    Resource resource = randomResource(rng);
                    for (int p = rng.nextInt(5); p >= 0; p--) {
                        List<KeyValue> attrs = randomAttributes(rng, 30);
                        gauge.addDataPoints(NumberDataPoint.newBuilder().setTimeUnixNano(1_000_000L).setAsInt(p).addAllAttributes(attrs));
                        pointAttrs.add(attrs);
                        pointResources.add(resource);
                    }
                    entries.add(new MetricEntry(Metric.newBuilder().setName("m").setGauge(gauge).build(), resource, null));
                }
                MetricBatchWriter.write(entries, root, false, stringCopy);

                assertEquals(pointAttrs.size(), root.getRowCount());
                List<MapVector> attrs = maps(root, OtelMetricSchema.COL_ATTRIBUTES, OtelMetricSchema.COL_TYPED_ATTRIBUTES);
                List<MapVector> res = maps(root, OtelMetricSchema.COL_RESOURCE_ATTRIBUTES, OtelMetricSchema.COL_TYPED_RESOURCE_ATTRIBUTES);
                for (int row = 0; row < pointAttrs.size(); row++) {
                    assertAttributeSet(attrs, row, pointAttrs.get(row), stringCopy, "batch " + batch + " point " + row);
                    Resource r = pointResources.get(row);
                    assertAttributeSet(res, row, r == null ? List.of() : r.getAttributesList(), stringCopy,
                            "batch " + batch + " resource point " + row);
                }
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    static Span randomSpan(Random rng) {
        Span.Builder span = Span.newBuilder().setName("op").addAllAttributes(randomAttributes(rng, 40));
        for (int i = rng.nextInt(5); i > 0; i--) {
            span.addEvents(Span.Event.newBuilder()
                    .setName(rng.nextInt(4) == 0 ? "" : "event-" + i)
                    .setTimeUnixNano(rng.nextInt(4) == 0 ? 0 : 1_000_000L * rng.nextInt(1_000_000))
                    .addAllAttributes(randomAttributes(rng, 8)));
        }
        for (int i = rng.nextInt(3); i > 0; i--) {
            byte[] traceId = new byte[rng.nextInt(4) == 0 ? 0 : 16];
            byte[] spanId = new byte[rng.nextInt(4) == 0 ? 0 : 8];
            rng.nextBytes(traceId);
            rng.nextBytes(spanId);
            span.addLinks(Span.Link.newBuilder()
                    .setTraceId(ByteString.copyFrom(traceId))
                    .setSpanId(ByteString.copyFrom(spanId))
                    .addAllAttributes(randomAttributes(rng, 6)));
        }
        return span.build();
    }

    private static String text(VarCharVector vector, int index) {
        return vector.isNull(index) ? null : new String(vector.get(index), StandardCharsets.UTF_8);
    }
}
