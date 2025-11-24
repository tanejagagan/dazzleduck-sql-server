package io.dazzleduck.sql.flight;

import io.micrometer.core.instrument.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;

    private final Counter getFlightInfo;
    private final Counter getFlightInfoPrepared;
    private final Counter getStreamStatement;
    private final Counter getStreamPreparedStatement;
    private final Counter bulkIngest;

    private final Timer getFlightInfoTimer;
    private final Timer getFlightInfoPreparedTimer;
    private final Timer getStreamStatementTimer;
    private final Timer getStreamPreparedStatementTimer;
    private final Timer bulkIngestTimer;

    private final Counter cancelCounterStatement;
    private final Counter cancelCounterPreparedStatement;

    private final AtomicInteger runningStatements = new AtomicInteger(0);
    private final AtomicInteger runningPrepared = new AtomicInteger(0);
    private final AtomicInteger runningBulkIngest = new AtomicInteger(0);

    private final AtomicLong startTime = new AtomicLong(0);

    public MicroMeterFlightRecorder(MeterRegistry registry, String producerId) {
        this.registry = registry;

        startTime.set(System.currentTimeMillis());

        this.getFlightInfo = counter("get_flight_info", producerId);
        this.getFlightInfoPrepared = counter("get_flight_info_prepared", producerId);
        this.getStreamStatement = counter("get_stream_statement", producerId);
        this.getStreamPreparedStatement = counter("get_stream_prepared_statement", producerId);
        this.bulkIngest = counter("bulk_ingest", producerId);
        this.cancelCounterStatement = counter("cancel_Statement_request", producerId);
        this.cancelCounterPreparedStatement = counter("cancel_Statement_Prepared_request", producerId);

        this.getFlightInfoTimer = timer("get_flight_info", producerId);
        this.getFlightInfoPreparedTimer = timer("get_flight_info_prepared", producerId);
        this.getStreamStatementTimer = timer("get_stream_statement", producerId);
        this.getStreamPreparedStatementTimer = timer("get_stream_prepared_statement", producerId);
        this.bulkIngestTimer = timer("bulk_ingest", producerId);

        registerGauge("start_time_ms", startTime, producerId);
        registerGauge("running_statements", runningStatements, producerId);
        registerGauge("running_prepared_statements", runningPrepared, producerId);
        registerGauge("running_bulk_ingest", runningBulkIngest, producerId);
    }

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

    @Override
    public void incrementRunningStatements() {

    }

    @Override
    public void decrementRunningStatements() {

    }

    @Override
    public void incrementRunningPrepared() {

    }

    @Override
    public void decrementRunningPrepared() {

    }

    @Override
    public void incrementRunningBulkIngest() {

    }

    @Override
    public void decrementRunningBulkIngest() {

    }

    @Override
    public void recordGetFlightInfoPreparedStatement() {

    }

    @Override public void recordGetStreamPreparedStatement(long size) { }
}
