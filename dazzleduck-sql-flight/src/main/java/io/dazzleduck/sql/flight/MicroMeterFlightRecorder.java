package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.micrometer.core.instrument.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;

    // -------------------- Counters -------------------------
    private final Counter streamStatementCounter;
    private final Counter streamPreparedStatementCounter;
    private final Counter bulkIngestCounter;

    private final Counter cancelStatementCounter;
    private final Counter cancelPreparedStatementCounter;

    // ---- Completed counters (Option 2) ----
    private final Counter streamStatementCompletedCounter;
    private final Counter streamPreparedStatementCompletedCounter;
    private final Counter bulkIngestCompletedCounter;

    // -------------------- Timers ---------------------------
    private final Timer streamStatementTimer;
    private final Timer streamPreparedStatementTimer;
    private final Timer bulkIngestTimer;

    // Start time
    private final AtomicLong startTime = new AtomicLong(0);

    // Thread-local timing
    private final ThreadLocal<Long> startNanos = ThreadLocal.withInitial(() -> 0L);

    public MicroMeterFlightRecorder(MeterRegistry registry, String producerId) {
        this.registry = registry;
        this.startTime.set(System.currentTimeMillis());

        // ------- Started counters --------

        this.streamStatementCounter = counter("stream_statement", producerId);
        this.streamPreparedStatementCounter = counter("stream_prepared_statement", producerId);
        this.bulkIngestCounter = counter("bulk_ingest", producerId);

        // ------- Cancelled counters -------
        this.cancelStatementCounter = counter("cancel_statement", producerId);
        this.cancelPreparedStatementCounter = counter("cancel_prepared_statement", producerId);

        // ------- Completed counters -------
        this.streamStatementCompletedCounter = counter("stream_statement_completed", producerId);
        this.streamPreparedStatementCompletedCounter = counter("stream_prepared_statement_completed", producerId);
        this.bulkIngestCompletedCounter = counter("bulk_ingest_completed", producerId);

        // ------- Timers -------
        this.streamStatementTimer = timer("stream_statement", producerId);
        this.streamPreparedStatementTimer = timer("stream_prepared_statement", producerId);
        this.bulkIngestTimer = timer("bulk_ingest", producerId);
    }

    // ==========================================================
    //                    HELPER BUILDERS
    // ==========================================================

    private Counter counter(String name, String producerId) {
        return Counter.builder("dazzleduck.flight." + name + ".count")
                .tag("producer", producerId)
                .register(registry);
    }

    private Timer timer(String name, String producerId) {
        return Timer.builder("dazzleduck.flight." + name + ".timer")
                .tag("producer", producerId)
                .publishPercentiles(0.5, 0.9, 0.99)
                .publishPercentileHistogram()
                .register(registry);
    }

    @Override
    public void recordStatementCancel() {
        cancelStatementCounter.increment();
    }

    @Override
    public void recordPreparedStatementCancel() {
        cancelPreparedStatementCounter.increment();
    }

    @Override
    public void startStreamStatement() {
        streamStatementCounter.increment();
        startNanos.set(System.nanoTime());
    }

    @Override
    public void endStreamStatement() {
        streamStatementTimer.record(System.nanoTime() - startNanos.get(), TimeUnit.NANOSECONDS);
        streamStatementCompletedCounter.increment();
    }

    // ---------------- Stream Prepared Statement ----------------

    @Override
    public void startStreamPreparedStatement() {
        streamPreparedStatementCounter.increment();
        startNanos.set(System.nanoTime());
    }

    @Override
    public void endStreamPreparedStatement() {
        streamPreparedStatementTimer.record(System.nanoTime() - startNanos.get(), TimeUnit.NANOSECONDS);
        streamPreparedStatementCompletedCounter.increment();
    }

    // ---------------- Bulk Ingest ----------------

    @Override
    public void startBulkIngest() {
        bulkIngestCounter.increment();
        startNanos.set(System.nanoTime());
    }

    @Override
    public void endBulkIngest() {
        bulkIngestTimer.record(System.nanoTime() - startNanos.get(), TimeUnit.NANOSECONDS);
        bulkIngestCompletedCounter.increment();
    }

    @Override
    public void recordGetStreamPreparedStatement(long size) { }


    @Override
    public FlightMetricsSnapshot snapshot() {

        long startTimeMs = startTime.get();
        int runningStatements = Math.toIntExact((long) streamStatementCounter.count() - (long) streamStatementCompletedCounter.count() - (long) cancelStatementCounter.count());
        int runningPrepared = Math.toIntExact((long) streamPreparedStatementCounter.count() - (long) streamPreparedStatementCompletedCounter.count() - (long) cancelPreparedStatementCounter.count());
        int runningBulkIngest = Math.toIntExact((long) bulkIngestCounter.count() - (long) bulkIngestCompletedCounter.count());

        double completedStatements = streamStatementCompletedCounter.count();
        double completedPrepared = streamPreparedStatementCompletedCounter.count();
        double completedBulkIngest = bulkIngestCompletedCounter.count();
        double cancelledStatements = cancelStatementCounter.count();
        double cancelledPrepared = cancelPreparedStatementCounter.count();

        return new FlightMetricsSnapshot(
                startTimeMs,
                runningStatements,
                runningPrepared,
                runningBulkIngest,
                completedStatements,
                completedPrepared,
                completedBulkIngest,
                cancelledStatements,
                cancelledPrepared

        );
    }
}
