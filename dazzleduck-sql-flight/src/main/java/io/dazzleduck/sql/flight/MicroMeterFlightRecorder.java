package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.StatementAudit;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer.CacheKey;
import io.dazzleduck.sql.flight.server.StatementContext;
import io.micrometer.core.instrument.*;
import org.slf4j.MarkerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;
    private static final Auditor auditor = new Auditor(MarkerFactory.getMarker("flight"));

    // ==================== INTERNAL REAL-TIME COUNTERS ====================
    // These LongAdders provide immediate, accurate counts for UI display
    private final LongAdder internalCancelStatementCount = new LongAdder();
    private final LongAdder internalCancelPreparedStatementCount = new LongAdder();
    private final LongAdder internalCompletedStatementCount = new LongAdder();
    private final LongAdder internalCompletedPreparedStatementCount = new LongAdder();
    private final LongAdder internalBytesOut = new LongAdder();

    // -------------------- Counters -------------------------
    private final Counter streamStatementCounter;
    private final Counter streamStatementErrorCounter;
    private final Counter stremStatementBytesOutCounter;
    private final Counter streamPreparedStatementErrorCounter;
    private final Counter streamPreparedStatementCounter;

    private final Counter stremPreparedStatementBytesOutCounter;

    private final Counter cancelStatementCounter;
    private final Counter cancelPreparedStatementCounter;

    private final Counter timeoutStatementCounter;

    private final Counter timeoutPreparedStatementCounter;

    // ---- Completed counters (Option 2) ----
    private final Counter streamStatementCompletedCounter;
    private final Counter streamPreparedStatementCompletedCounter;
    private final Counter bulkIngestCompletedCounter;

    private final Counter statementStart;
    private final Counter statementEnd;
    private final Counter statementError;

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
        this.streamPreparedStatementErrorCounter = counter("stream_prepared_statement_error", producerId);
        this.streamStatementErrorCounter = counter("stream_statement_error", producerId);
        this.timeoutStatementCounter = counter("stream_statement_timeout", producerId);
        this.timeoutPreparedStatementCounter = counter("stream_prepared_statement_timeout", producerId);

        // ------- Cancelled counters -------
        this.cancelStatementCounter = counter("cancel_statement", producerId);
        this.cancelPreparedStatementCounter = counter("cancel_prepared_statement", producerId);

        // ------- Completed counters -------
        this.streamStatementCompletedCounter = counter("stream_statement_completed", producerId);
        this.streamPreparedStatementCompletedCounter = counter("stream_prepared_statement_completed", producerId);
        this.bulkIngestCompletedCounter = counter("bulk_ingest_completed", producerId);
        this.stremPreparedStatementBytesOutCounter = counter("stream_prepared_statement_bytes_out", producerId);
        this.stremStatementBytesOutCounter = counter("stream_statement_bytes_out", producerId);
        this.statementStart = counter("statement_start", producerId);
        this.statementEnd = counter("statement_end", producerId);
        this.statementError = counter("statement_error", producerId);
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
    public void recordStatementCancel(CacheKey key, StatementContext<?> ctx) {
        internalCancelStatementCount.increment();
        cancelStatementCounter.increment();
        auditor.audit(buildAudit(key, ctx, "CANCEL", null));
    }

    @Override
    public void recordPreparedStatementCancel(CacheKey key, StatementContext<?> ctx) {
        internalCancelPreparedStatementCount.increment();
        cancelPreparedStatementCounter.increment();
        auditor.audit(buildAudit(key, ctx, "CANCEL", null));
    }

    @Override
    public void recordStatementStreamStart(CacheKey key, StatementContext<?> ctx) {
        statementStart.increment();
        auditor.audit(buildAudit(key, ctx, "START", null));
    }

    @Override
    public void recordStatementStreamEnd(CacheKey key, StatementContext<?> ctx) {
        statementEnd.increment();
        auditor.audit(buildAudit(key, ctx, "END", null));
    }

    @Override
    public void recordStatementStreamError(CacheKey key, StatementContext<?> ctx, Throwable error) {
        statementError.increment();
        String errorMessage = error.getClass().getSimpleName() + ": " + error.getMessage();
        auditor.audit(buildAudit(key, ctx, "ERROR", errorMessage));
    }

    @Override
    public void recordStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        timeoutStatementCounter.increment();
        auditor.audit(buildAudit(key, ctx, "TIMEOUT", null));
    }

    @Override
    public void recordPreparedStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        timeoutPreparedStatementCounter.increment();
        auditor.audit(buildAudit(key, ctx, "TIMEOUT", null));
    }

    @Override
    public void startStreamStatement() {
        streamStatementCounter.increment();
    }

    @Override
    public void endStreamStatement() {
        internalCompletedStatementCount.increment();
        streamStatementCompletedCounter.increment();
    }

    @Override
    public void errorStreamStatement() {
        streamStatementErrorCounter.increment();
    }

    @Override
    public void errorStreamPreparedStatement() {
        streamPreparedStatementErrorCounter.increment();
    }

    // ---------------- Stream Prepared Statement ----------------

    @Override
    public void startStreamPreparedStatement() {
        streamPreparedStatementCounter.increment();
    }

    @Override
    public void endStreamPreparedStatement() {
        internalCompletedPreparedStatementCount.increment();
        streamPreparedStatementCompletedCounter.increment();
    }

    @Override
    public void errorPreparedStreamStatement() {
        streamStatementErrorCounter.increment();
    }




    @Override
    public void recordGetStreamPreparedStatement(long size) {
        if (size > 0) {
            internalBytesOut.add(size);
        }
        stremPreparedStatementBytesOutCounter.increment(size);
    }

    @Override
    public void recordGetStreamStatement(long size) {
        if (size > 0) {
            internalBytesOut.add(size);
        }
        stremStatementBytesOutCounter.increment(size);
    }

    @Override
    public void registerWriteQueue(String identifier, Map<String, LongSupplier> counters)  {
        counters.forEach((name, supplier) -> {
            FunctionCounter.builder("dazzleduck.flight.ingest_queue." + name, supplier, LongSupplier::getAsLong)
                    .tag("identifier", identifier)
                    .register(registry);
        });
    }


    @Override
    public double getBytesOut() {
        return internalBytesOut.sum();
    }

    @Override
    public double getBytesIn() {
        return 0;
    }

    @Override
    public long getCompletedStatements() {
        return internalCompletedStatementCount.sum();
    }
    @Override
    public long getCompletedPreparedStatements() {
        return internalCompletedPreparedStatementCount.sum();
    }

    @Override
    public long getCancelledStatements() {
        return internalCancelStatementCount.sum();
    }
    @Override
    public long getCancelledPreparedStatements() {
        return internalCancelPreparedStatementCount.sum();
    }

    // ==========================================================
    //                    HELPER BUILDER
    // ==========================================================
    private static StatementAudit buildAudit(CacheKey key, StatementContext<?> ctx, String action, String error) {
        return new StatementAudit(
                key.id(),
                key.peerIdentity(),
                action,
                ctx.isPreparedStatementContext(),
                ctx.getQuery(),
                ctx.startTime(),
                ctx.endTime(),
                ctx.bytesOut(),
                error
        );
    }
}
