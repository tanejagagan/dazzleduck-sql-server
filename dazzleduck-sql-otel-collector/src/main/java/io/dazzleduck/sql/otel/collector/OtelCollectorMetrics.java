package io.dazzleduck.sql.otel.collector;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/**
 * Micrometer metrics for the OTLP collector. All meters are keyed by queue ID so
 * each configured ingestion queue gets its own counters and timers.
 *
 * <p>Meters are created on first use ({@link #recordExport}/{@link #recordError}) and on
 * {@link #registerWriter} — no pre-allocation at startup. This means a queue that never
 * receives traffic produces no meters.
 *
 * <p>All meters carry a {@code queue} tag with the queue ID as the value, plus the common
 * tags ({@code service.name}, {@code host.name}, {@code container.id}) set once at startup
 * via {@code registry.config().commonTags()} in {@link OtelCollectorServer}.
 *
 * <pre>
 *   dazzleduck.otel.export.requests        {queue=<id>}  – RPC calls received
 *   dazzleduck.otel.export.records         {queue=<id>}  – individual records exported
 *   dazzleduck.otel.export.errors          {queue=<id>}  – failed exports
 *   dazzleduck.otel.export.latency         {queue=<id>}  – end-to-end RPC latency (p50/p95/p99)
 *   dazzleduck.otel.writer.bytes_written   {queue=<id>}  – cumulative bytes written to Parquet
 *   dazzleduck.otel.writer.batches_written {queue=<id>}  – batches flushed to Parquet
 *   dazzleduck.otel.writer.pending_batches {queue=<id>}  – queue depth (batches not yet written)
 *   dazzleduck.otel.writer.pending_buckets {queue=<id>}  – bucket queue depth
 * </pre>
 */
public class OtelCollectorMetrics implements Closeable {

    private final MeterRegistry registry;
    private final List<Meter> registeredMeters = new CopyOnWriteArrayList<>();

    private final ConcurrentHashMap<String, LongAdder> requestsPerQueue = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> recordsPerQueue  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> errorsPerQueue   = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Timer>     timersPerQueue   = new ConcurrentHashMap<>();

    public OtelCollectorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // -----------------------------------------------------------------------
    // Sample factory — called at the start of each export() RPC handler
    // -----------------------------------------------------------------------

    public Timer.Sample startSample() {
        return Timer.start(registry);
    }

    // -----------------------------------------------------------------------
    // Recording methods — called by the three OTLP services
    // -----------------------------------------------------------------------

    public void recordExport(String queueId, int count, Timer.Sample sample) {
        getOrCreateRequests(queueId).increment();
        getOrCreateRecords(queueId).add(count);
        sample.stop(getOrCreateTimer(queueId));
    }

    public void recordError(String queueId, Timer.Sample sample) {
        getOrCreateErrors(queueId).increment();
        sample.stop(getOrCreateTimer(queueId));
    }

    // -----------------------------------------------------------------------
    // Writer metrics — registered once per queue at startup
    // -----------------------------------------------------------------------

    public void registerWriter(String queueId, SignalWriter writer) {
        registeredMeters.add(FunctionCounter.builder("dazzleduck.otel.writer.bytes_written", writer,
                        w -> w.getStats().totalWriteBytes())
                .tag("queue", queueId)
                .description("Cumulative bytes written to Parquet")
                .register(registry));
        registeredMeters.add(FunctionCounter.builder("dazzleduck.otel.writer.batches_written", writer,
                        w -> w.getStats().totalWriteBatches())
                .tag("queue", queueId)
                .description("Cumulative number of Arrow batches flushed to Parquet")
                .register(registry));
        registeredMeters.add(Gauge.builder("dazzleduck.otel.writer.pending_batches", writer,
                        w -> w.getStats().pendingBatches())
                .tag("queue", queueId)
                .description("Current number of batches queued but not yet written")
                .register(registry));
        registeredMeters.add(Gauge.builder("dazzleduck.otel.writer.pending_buckets", writer,
                        w -> w.getStats().pendingBuckets())
                .tag("queue", queueId)
                .description("Current number of buckets queued but not yet written")
                .register(registry));
    }

    // -----------------------------------------------------------------------
    // Lazy meter creation — one set of counters + one timer per queue ID
    // -----------------------------------------------------------------------

    private LongAdder getOrCreateRequests(String queueId) {
        return requestsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            registeredMeters.add(FunctionCounter.builder("dazzleduck.otel.export.requests", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of OTLP export RPC calls received")
                    .register(registry));
            return adder;
        });
    }

    private LongAdder getOrCreateRecords(String queueId) {
        return recordsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            registeredMeters.add(FunctionCounter.builder("dazzleduck.otel.export.records", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of individual records exported")
                    .register(registry));
            return adder;
        });
    }

    private LongAdder getOrCreateErrors(String queueId) {
        return errorsPerQueue.computeIfAbsent(queueId, id -> {
            var adder = new LongAdder();
            registeredMeters.add(FunctionCounter.builder("dazzleduck.otel.export.errors", adder, LongAdder::sum)
                    .tag("queue", id)
                    .description("Number of failed export requests")
                    .register(registry));
            return adder;
        });
    }

    private Timer getOrCreateTimer(String queueId) {
        return timersPerQueue.computeIfAbsent(queueId, id ->
                Timer.builder("dazzleduck.otel.export.latency")
                        .tag("queue", id)
                        .description("End-to-end export RPC duration — from request received to onCompleted/onError")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(registry));
    }

    // -----------------------------------------------------------------------
    // Real-time accessors (for testing)
    // -----------------------------------------------------------------------

    public long getRequests(String queueId) {
        var a = requestsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    public long getRecords(String queueId) {
        var a = recordsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    public long getErrors(String queueId) {
        var a = errorsPerQueue.get(queueId);
        return a != null ? a.sum() : 0L;
    }

    public MeterRegistry getRegistry() { return registry; }

    /** Removes all registered meters from the registry so SignalWriter references can be GC'd. */
    @Override
    public void close() {
        registeredMeters.forEach(registry::remove);
        registeredMeters.clear();
    }
}
