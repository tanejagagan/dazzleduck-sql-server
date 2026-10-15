package io.dazzleduck.sql.otel.collector;

import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.SECRET_KEY_BASE64;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.bearerMetadata;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.findFreePort;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.noopHandler;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.smallBucketConfig;
import static io.dazzleduck.sql.otel.collector.OtelCollectorCustomQueueTest.tokenWithQueueClaim;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end: attributes of every OTLP value type land in their typed map columns of the written
 * Parquet with their native DuckDB types, and {@code attributes.string_copy} decides whether the
 * string map also carries the non-string values.
 */
public class OtelCollectorTypedAttributesTest {

    /** One attribute of each kind; the resource and span events/links reuse the same set. */
    static final List<KeyValue> ATTRS = List.of(
            kv("s", AnyValue.newBuilder().setStringValue("text").build()),
            kv("n", AnyValue.newBuilder().setIntValue(42).build()),
            kv("d", AnyValue.newBuilder().setDoubleValue(1.5).build()),
            kv("f", AnyValue.newBuilder().setBoolValue(true).build()),
            kv("b", AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(new byte[]{1, 2, (byte) 0xff})).build()),
            kv("arr", AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder()
                    .addValues(AnyValue.newBuilder().setIntValue(1))
                    .addValues(AnyValue.newBuilder().setStringValue("two"))
                    .addValues(AnyValue.newBuilder().setBoolValue(false))).build()),
            kv("obj", AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder()
                    .addValues(kv("k", AnyValue.newBuilder().setDoubleValue(2.5).build()))).build()));

    private static final Resource RESOURCE = Resource.newBuilder().addAllAttributes(ATTRS).build();

    @BeforeAll
    static void loadExtensions() throws Exception {
        ConnectionPool.executeBatch(new String[]{"INSTALL arrow FROM community", "LOAD arrow"});
    }

    static String text(String sql) throws Exception {
        return ConnectionPool.collectFirst(sql, String.class);
    }

    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    abstract static class ServerFixture {

        Path outputRoot;
        OtelCollectorServer server;
        ManagedChannel channel;

        abstract boolean stringCopy();

        @BeforeAll
        void setup() throws Exception {
            outputRoot = Files.createTempDirectory("otel-typed-attrs-test");
            for (String q : new String[]{"logs", "traces", "metrics"}) {
                Files.createDirectories(outputRoot.resolve(q));
            }
            var props = new CollectorProperties();
            props.setShutdownGracePeriod(Duration.ZERO);
            props.setGrpcPort(findFreePort());
            props.setIngestionHandler(noopHandler(id -> outputRoot.resolve(id).toString(), false,
                    "logs", "traces", "metrics"));
            props.setIngestionConfig(smallBucketConfig());
            props.setAuthentication("jwt");
            props.setSecretKey(SECRET_KEY_BASE64);
            props.setAttributesStringCopy(stringCopy());

            server = new OtelCollectorServer(props);
            server.start();
            channel = ManagedChannelBuilder.forAddress("localhost", props.getGrpcPort()).usePlaintext().build();

            LogsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("logs")).export(
                    ExportLogsServiceRequest.newBuilder().addResourceLogs(ResourceLogs.newBuilder()
                            .setResource(RESOURCE)
                            .addScopeLogs(ScopeLogs.newBuilder().addLogRecords(LogRecord.newBuilder()
                                    .setTimeUnixNano(1_000_000_000L).addAllAttributes(ATTRS)))).build());
            TraceServiceGrpc.newBlockingStub(channel).withInterceptors(auth("traces")).export(
                    ExportTraceServiceRequest.newBuilder().addResourceSpans(ResourceSpans.newBuilder()
                            .setResource(RESOURCE)
                            .addScopeSpans(ScopeSpans.newBuilder().addSpans(Span.newBuilder()
                                    .setName("op").addAllAttributes(ATTRS)
                                    .addEvents(Span.Event.newBuilder().setName("ev").addAllAttributes(ATTRS))
                                    .addEvents(Span.Event.newBuilder().setName("bare"))
                                    .addLinks(Span.Link.newBuilder().addAllAttributes(ATTRS))))).build());
            MetricsServiceGrpc.newBlockingStub(channel).withInterceptors(auth("metrics")).export(
                    ExportMetricsServiceRequest.newBuilder().addResourceMetrics(ResourceMetrics.newBuilder()
                            .setResource(RESOURCE)
                            .addScopeMetrics(ScopeMetrics.newBuilder().addMetrics(Metric.newBuilder()
                                    .setName("m").setGauge(Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(1_000_000_000L).setAsInt(1).addAllAttributes(ATTRS)))))).build());
        }

        @AfterAll
        void cleanup() throws Exception {
            try {
                if (channel != null) { channel.shutdown(); channel.awaitTermination(5, TimeUnit.SECONDS); }
            } finally {
                if (server != null) server.close();
            }
        }

        private static io.grpc.ClientInterceptor auth(String queue) {
            return MetadataUtils.newAttachHeadersInterceptor(bearerMetadata(tokenWithQueueClaim(queue)));
        }

        String from(String queue) {
            return " FROM read_parquet('%s/%s/*.parquet')".formatted(outputRoot.toString().replace('\\', '/'), queue);
        }

        /** Asserts the typed maps of attribute set {@code prefix} (a column or a struct field). */
        void assertTyped(String queue, String prefix) throws Exception {
            String sql = "SELECT concat_ws('|', "
                    + "typeof(%1$s_int['n']), %1$s_int['n'], "
                    + "typeof(%1$s_double['d']), %1$s_double['d'], "
                    + "typeof(%1$s_bool['f']), %1$s_bool['f'], "
                    + "typeof(%1$s_bytes['b']), hex(%1$s_bytes['b']), "
                    + "typeof(%1$s_json['arr']), %1$s_json['arr'], %1$s_json['obj'] ->> '$.k', "
                    + "%1$s['s'])";
            assertEquals("BIGINT|42|DOUBLE|1.5|BOOLEAN|true|BLOB|0102FF|JSON|[1,\"two\",false]|2.5|text",
                    text(sql.formatted(prefix) + from(queue)), queue + " " + prefix);
        }

        /** Renders the string map of {@code prefix} as {@code k=v;...}, sorted by key. */
        String stringMap(String queue, String prefix) throws Exception {
            return text(("SELECT string_agg(e.key || '=' || e.value, ';' ORDER BY e.key) "
                    + "FROM (SELECT unnest(map_entries(%s)) AS e %s)").formatted(prefix, from(queue)));
        }

        @Test
        void logsHaveTypedMaps() throws Exception {
            assertTyped("logs", "attributes");
            assertTyped("logs", "resource_attributes");
        }

        @Test
        void tracesHaveTypedMaps() throws Exception {
            assertTyped("traces", "attributes");
            assertTyped("traces", "resource_attributes");
            assertTyped("traces", "events[1].attributes");
            assertTyped("traces", "links[1].attributes");
            assertEquals("NULL|NULL", text("SELECT concat_ws('|', coalesce(events[2].attributes_int::VARCHAR, 'NULL'), "
                    + "coalesce(events[2].attributes::VARCHAR, 'NULL'))" + from("traces")),
                    "an event without attributes leaves every map null");
        }

        @Test
        void metricsHaveTypedMaps() throws Exception {
            assertTyped("metrics", "attributes");
            assertTyped("metrics", "resource_attributes");
        }
    }

    @Nested
    class WithStringCopy extends ServerFixture {
        @Override boolean stringCopy() { return true; }

        @Test
        void stringMapHoldsEveryValue() throws Exception {
            String expected = "arr=[1,\"two\",false];b=0102ff;d=1.5;f=true;n=42;obj={\"k\":2.5};s=text";
            for (String queue : List.of("logs", "traces", "metrics")) {
                assertEquals(expected, stringMap(queue, "attributes"), queue);
                assertEquals(expected, stringMap(queue, "resource_attributes"), queue);
            }
            assertEquals(expected, stringMap("traces", "events[1].attributes"));
        }
    }

    @Nested
    class WithoutStringCopy extends ServerFixture {
        @Override boolean stringCopy() { return false; }

        @Test
        void stringMapHoldsOnlyStrings() throws Exception {
            for (String queue : List.of("logs", "traces", "metrics")) {
                assertEquals("s=text", stringMap(queue, "attributes"), queue);
                assertEquals("s=text", stringMap(queue, "resource_attributes"), queue);
            }
            assertEquals("s=text", stringMap("traces", "events[1].attributes"));
            assertEquals("s=text", stringMap("traces", "links[1].attributes"));
        }
    }

    @Test
    void configDefaultsToStringCopy() {
        assertTrue(new CollectorConfig().getAttributesStringCopy());
        assertTrue(new CollectorProperties().isAttributesStringCopy(),
                "the programmatic default must match reference.conf");
    }

    @Test
    void configCanTurnStringCopyOff() {
        var config = ConfigFactory.parseString("otel_collector.attributes.string_copy = false")
                .withFallback(ConfigFactory.load()).resolve();
        assertFalse(new CollectorConfig(config).toProperties().isAttributesStringCopy());
    }

    @Test
    void jsonKeepsTypes() {
        AnyValue nested = AnyValue.newBuilder().setArrayValue(ArrayValue.newBuilder()
                .addValues(AnyValue.newBuilder().setDoubleValue(Double.NaN))
                .addValues(AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(new byte[]{1, 2})))
                .addValues(AnyValue.getDefaultInstance())
                .addValues(AnyValue.newBuilder().setStringValue("a\"b\u0001"))
                .addValues(AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder()
                        .addValues(kv("x", AnyValue.newBuilder().setIntValue(-7).build()))))).build();
        assertEquals("[\"NaN\",\"AQI=\",null,\"a\\\"b\\u0001\",{\"x\":-7}]", LogRecordConverter.anyValueToJson(nested));
    }

    static KeyValue kv(String key, AnyValue value) {
        return KeyValue.newBuilder().setKey(key).setValue(value).build();
    }
}
