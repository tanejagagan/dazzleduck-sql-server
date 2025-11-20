package io.dazzleduck.sql.micrometer.metrics;


import io.micrometer.core.instrument.*;

import java.util.concurrent.Callable;

public final class FlightMetrics {

    private final MeterRegistry registry;

    // Counters
    private final Counter getFlightInfo;
    private final Counter getFlightInfoPrepared;
    private final Counter getStreamStatement;
    private final Counter getStreamPreparedStatement;
    private final Counter bulkIngest;
    private final Counter putPreparedStatementUpdate;

    // Timers
    private final Timer getFlightInfoTimer;
    private final Timer getStreamStatementTimer;
    private final Timer getStreamPreparedStatementTimer;
    private final Timer bulkIngestTimer;

    public FlightMetrics(MeterRegistry registry, String producerId) {
        this.registry = registry;

        this.getFlightInfo = counter("get_flight_info", producerId);
        this.getFlightInfoPrepared = counter("get_flight_info_prepared", producerId);
        this.getStreamStatement = counter("get_stream_statement", producerId);
        this.getStreamPreparedStatement = counter("get_stream_prepared_statement", producerId);
        this.bulkIngest = counter("bulk_ingest", producerId);
        this.putPreparedStatementUpdate = counter("put_prepared_update", producerId);

        this.getFlightInfoTimer = timer("get_flight_info", producerId);
        this.getStreamStatementTimer = timer("get_stream_statement", producerId);
        this.getStreamPreparedStatementTimer = timer("get_stream_prepared_statement", producerId);
        this.bulkIngestTimer = timer("bulk_ingest", producerId);
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
                .register(registry);
    }

    // ----------------------------
    // Wrappers for metrics
    // ----------------------------

    public <T> T recordGetFlightInfo(Callable<T> work) throws Exception {
        getFlightInfo.increment();
        return getFlightInfoTimer.recordCallable(work);
    }

    public <T> T recordGetFlightInfoPrepared(Callable<T> work) throws Exception {
        getFlightInfoPrepared.increment();
        return getFlightInfoTimer.recordCallable(work);
    }

    public void recordGetStreamStatement(Runnable r) {
        getStreamStatement.increment();
        getStreamStatementTimer.record(r);
    }

    public void recordGetStreamPreparedStatement(Runnable r) {
        getStreamPreparedStatement.increment();
        getStreamPreparedStatementTimer.record(r);
    }

    public void recordBulkIngest(Runnable r) {
        bulkIngest.increment();
        bulkIngestTimer.record(r);
    }

    public void incrementPreparedStatementUpdate() {
        putPreparedStatementUpdate.increment();
    }
}
