package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.micrometer.core.instrument.*;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;

    // --------------------- Counters --------------------------
    private final Counter getFlightInfo;
    private final Counter getFlightInfoPrepared;
    private final Counter getStreamStatement;
    private final Counter getStreamPreparedStatement;
    private final Counter bulkIngest;

    private final Counter cancelCounterStatement;
    private final Counter cancelCounterPreparedStatement;

    // --------------------- Timers ----------------------------
    private final Timer getFlightInfoTimer;
    private final Timer getFlightInfoPreparedTimer;
    private final Timer getStreamStatementTimer;
    private final Timer getStreamPreparedStatementTimer;
    private final Timer bulkIngestTimer;

    // --------------------- Running Gauges ---------------------
    private final AtomicInteger runningStatements = new AtomicInteger(0);
    private final AtomicInteger runningPrepared = new AtomicInteger(0);
    private final AtomicInteger runningBulkIngest = new AtomicInteger(0);

    private final AtomicLong startTime = new AtomicLong(0);

    // --------------------- Network Metrics --------------------
    private final Counter dataInBytes;
    private final Counter dataOutBytes;
    private final Counter arrowBatchIn;
    private final Counter arrowBatchOut;

    // ==========================================================
    //                    CONSTRUCTOR
    // ==========================================================

    public MicroMeterFlightRecorder(MeterRegistry registry, String producerId) {
        this.registry = registry;

        startTime.set(System.currentTimeMillis());

        // ----- Counters -----
        this.getFlightInfo = counter("get_flight_info", producerId);
        this.getFlightInfoPrepared = counter("get_flight_info_prepared", producerId);
        this.getStreamStatement = counter("get_stream_statement", producerId);
        this.getStreamPreparedStatement = counter("get_stream_prepared_statement", producerId);
        this.bulkIngest = counter("bulk_ingest", producerId);

        this.cancelCounterStatement = counter("cancel_statement_request", producerId);
        this.cancelCounterPreparedStatement = counter("cancel_statement_prepared_request", producerId);

        // ----- Timers -----
        this.getFlightInfoTimer = timer("get_flight_info", producerId);
        this.getFlightInfoPreparedTimer = timer("get_flight_info_prepared", producerId);
        this.getStreamStatementTimer = timer("get_stream_statement", producerId);
        this.getStreamPreparedStatementTimer = timer("get_stream_prepared_statement", producerId);
        this.bulkIngestTimer = timer("bulk_ingest", producerId);

        // ----- Gauges -----
        registerGauge("start_time_ms", startTime, producerId);
        registerGauge("running_statements", runningStatements, producerId);
        registerGauge("running_prepared_statements", runningPrepared, producerId);
        registerGauge("running_bulk_ingest", runningBulkIngest, producerId);

        // ----- Network Metrics -----
        this.dataInBytes = Counter.builder("dazzleduck.flight.data_in.bytes")
                .tag("producer", producerId)
                .register(registry);

        this.dataOutBytes = Counter.builder("dazzleduck.flight.data_out.bytes")
                .tag("producer", producerId)
                .register(registry);

        this.arrowBatchIn = Counter.builder("dazzleduck.flight.arrow_batch.in")
                .tag("producer", producerId)
                .register(registry);

        this.arrowBatchOut = Counter.builder("dazzleduck.flight.arrow_batch.out")
                .tag("producer", producerId)
                .register(registry);
    }

    // ==========================================================
    //                      METRIC HELPERS
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

    private void registerGauge(String name, AtomicInteger ref, String producerId) {
        Gauge.builder("dazzleduck.flight." + name, ref, AtomicInteger::get)
                .tag("producer", producerId)
                .register(registry);
    }

    private void registerGauge(String name, AtomicLong ref, String producerId) {
        Gauge.builder("dazzleduck.flight." + name, ref, AtomicLong::get)
                .tag("producer", producerId)
                .register(registry);
    }

    // ==========================================================
    //                       STATEMENT RECORDERS
    // ==========================================================

    @Override
    public void recordStatementCancel() { cancelCounterStatement.increment(); }

    @Override
    public void recordPreparedStatementCancel() { cancelCounterPreparedStatement.increment(); }

    @Override
    public <T> T recordGetFlightInfo(Callable<T> work) throws Exception {
        getFlightInfo.increment();
        return getFlightInfoTimer.recordCallable(work);
    }

    @Override
    public <T> T recordGetFlightInfoPrepared(Callable<T> work) throws Exception {
        getFlightInfoPrepared.increment();
        return getFlightInfoPreparedTimer.recordCallable(work);
    }

    @Override
    public void recordGetStreamStatement(Runnable r) {
        runningStatements.incrementAndGet();
        try {
            getStreamStatement.increment();
            getStreamStatementTimer.record(r);
        } finally {
            runningStatements.decrementAndGet();
        }
    }

    @Override
    public void recordGetStreamPreparedStatement(Runnable r) {
        runningPrepared.incrementAndGet();
        long start = System.nanoTime();
        try {
            getStreamPreparedStatement.increment();
            r.run();
        } finally {
            long end = System.nanoTime();
            getStreamPreparedStatementTimer.record(end - start, TimeUnit.NANOSECONDS);
            runningPrepared.decrementAndGet();
        }
    }

    @Override
    public void recordBulkIngest(Runnable r) {
        runningBulkIngest.incrementAndGet();
        try {
            bulkIngest.increment();
            bulkIngestTimer.record(r);
        } finally {
            runningBulkIngest.decrementAndGet();
        }
    }

    @Override public void recordGetFlightInfoStatement() { }
    @Override public void recordGetFlightInfoPreparedStatement() { }
    @Override public void recordGetStreamPreparedStatement(long size) { }



    // ==========================================================
    //                   NETWORK RECORDERS
    // ==========================================================

    public void recordDataIn(long bytes) {
        dataInBytes.increment(bytes);
        arrowBatchIn.increment();
    }

    public void recordDataOut(long bytes) {
        dataOutBytes.increment(bytes);
        arrowBatchOut.increment();
    }

    // ==========================================================
    //                 GETTERS USED BY UI / SNAPSHOT
    // ==========================================================

    public long getStartTimeMs() { return startTime.get(); }

    public int getRunningStatements() { return runningStatements.get(); }

    public int getRunningPrepared() { return runningPrepared.get(); }

    public int getRunningBulkIngest() { return runningBulkIngest.get(); }

    public double getCompletedStatements() { return getStreamStatement.count(); }

    public double getCompletedPreparedStatements() { return getStreamPreparedStatement.count(); }

    public double getCompletedBulkIngest() { return bulkIngest.count(); }

    public double getCancelledStatements() { return cancelCounterStatement.count(); }

    public double getCancelledPreparedStatements() { return cancelCounterPreparedStatement.count(); }

    public double getDataInBytes() { return dataInBytes.count(); }

    public double getDataOutBytes() { return dataOutBytes.count(); }

    public double getArrowBatchIn() { return arrowBatchIn.count(); }

    public double getArrowBatchOut() { return arrowBatchOut.count(); }

    // ==========================================================
    //                       SNAPSHOT EXPORT
    // ==========================================================

    public FlightMetricsSnapshot snapshot() {
        FlightMetricsSnapshot s = new FlightMetricsSnapshot();

        s.startTimeMs = getStartTimeMs();

        s.runningStatements = getRunningStatements();
        s.runningPrepared = getRunningPrepared();
        s.runningBulkIngest = getRunningBulkIngest();

        s.completedStatements = getCompletedStatements();
        s.completedPrepared = getCompletedPreparedStatements();
        s.completedBulkIngest = getCompletedBulkIngest();

        s.cancelledStatements = getCancelledStatements();
        s.cancelledPrepared = getCancelledPreparedStatements();

        s.dataInBytes = getDataInBytes();
        s.dataOutBytes = getDataOutBytes();
        return s;
    }
}
