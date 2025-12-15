package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.StatementAuditObject;
import io.dazzleduck.sql.flight.server.StatementContext;
import io.micrometer.core.instrument.*;
import org.slf4j.MarkerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;
    private static final Auditor auditor = new Auditor(MarkerFactory.getMarker("flight"));

    // -------------------- Counters -------------------------
    private final Counter streamStatementCounter;
    private final Counter streamStatementErrorCounter;
    private final Counter stremStatementBytesOutCounter;
    private final Counter streamPreparedStatementErrorCounter;
    private final Counter streamPreparedStatementCounter;

    private final Counter stremPreparedStatementBytesOutCounter;
    private final Counter bulkIngestCounter;

    private final Counter bulkIngestErrorCounter;

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
        this.bulkIngestCounter = counter("bulk_ingest", producerId);
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
        this.bulkIngestErrorCounter = counter("bulk_ingest_error", producerId);

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
    public void recordStatementCancel(StatementContext<?> ctx) {
        cancelStatementCounter.increment();
        auditor.audit(buildAudit(ctx, "CANCEL", null));
    }

    @Override
    public void recordPreparedStatementCancel(StatementContext<?> ctx) {
        cancelPreparedStatementCounter.increment();
        auditor.audit(buildAudit(ctx, "CANCEL", null));
    }

    @Override
    public void recordStatementStart(StatementContext<?> ctx) {
        statementStart.increment();
        auditor.audit(buildAudit(ctx, "START", null));
    }

    @Override
    public void recordStatementEnd(StatementContext<?> ctx) {
        statementEnd.increment();
        auditor.audit(buildAudit(ctx, "END", null));
    }

    @Override
    public void recordStatementError(StatementContext<?> ctx, Throwable error) {
        statementError.increment();
        String errorMessage = error.getClass().getSimpleName() + ": " + error.getMessage();
        auditor.audit(buildAudit(ctx, "ERROR", errorMessage));
    }

    @Override
    public void recordStatementTimeout(StatementContext<?> ctx) {
        timeoutStatementCounter.increment();
        auditor.audit(buildAudit(ctx, "TIMEOUT", null));
    }

    @Override
    public void recordPreparedStatementTimeout(StatementContext<?> ctx) {
        timeoutPreparedStatementCounter.increment();
        auditor.audit(buildAudit(ctx, "TIMEOUT", null));
    }

    @Override
    public void recordStreamStart(StatementContext<?> ctx) {
        auditor.audit(buildAudit(ctx, "STREAM_START", null));
    }

    @Override
    public void recordStreamEnd(StatementContext<?> ctx) {
        auditor.audit(buildAudit(ctx, "STREAM_END", null));
    }

    @Override
    public void recordStreamError(StatementContext<?> ctx, Throwable error) {
        String msg = error.getClass().getSimpleName() + ": " + error.getMessage();
        auditor.audit(buildAudit(ctx, "STREAM_ERROR", msg));
    }

    @Override
    public void startStreamStatement() {
        streamStatementCounter.increment();
    }

    @Override
    public void endStreamStatement() {
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
        streamPreparedStatementCompletedCounter.increment();
    }

    @Override
    public void errorPreparedStreamStatement() {
        streamStatementErrorCounter.increment();
    }

    // ---------------- Bulk Ingest ----------------

    @Override
    public void startBulkIngest() {
        startNanos.set(System.nanoTime());
    }

    @Override
    public void endBulkIngest() {
        bulkIngestCompletedCounter.increment();
    }

    @Override
    public void errorBulkIngest() {
        bulkIngestErrorCounter.increment();
    }

    @Override
    public void recordGetStreamPreparedStatement(long size) {
        stremPreparedStatementBytesOutCounter.increment(size);
    }

    @Override
    public void recordGetStreamStatement(long size) {
        stremStatementBytesOutCounter.increment(size);
    }


    @Override
    public double getBytesOut() {
        return stremStatementBytesOutCounter.count() + stremPreparedStatementBytesOutCounter.count();
    }

    @Override
    public double getBytesIn() {
        return 0;
    }

    @Override
    public long getCompletedStatements() {
        return (long) streamStatementCompletedCounter.count();
    }
    @Override
    public long getCompletedPreparedStatements() {
        return (long) streamPreparedStatementCompletedCounter.count();
    }
    @Override
    public long getCompletedBulkIngest() {
        return (long) bulkIngestCompletedCounter.count();
    }
    @Override
    public long getCancelledStatements() {
        return (long) cancelStatementCounter.count();
    }
    @Override
    public long getCancelledPreparedStatements() {
        return (long) cancelPreparedStatementCounter.count();
    }

    // ==========================================================
    //                    HELPER BUILDER
    // ==========================================================
    private static StatementAuditObject buildAudit(StatementContext<?> ctx, String action, String error) {
        return new StatementAuditObject(
                ctx.getStatementId(),
                ctx.getUser(),
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
