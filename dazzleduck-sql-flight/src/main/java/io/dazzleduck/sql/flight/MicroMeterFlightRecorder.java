package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.model.StatementAudit;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer.CacheKey;
import io.dazzleduck.sql.flight.server.StatementContext;
import io.micrometer.core.instrument.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MarkerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Flight metrics recorder implementing a dual-stream pattern for real-time and time-series monitoring.
 * <p>
 * Architecture Overview:
 * - LongAdders serve as the real-time source of truth for UI and operational queries
 * - FunctionCounters bridge LongAdders to Micrometer for external monitoring (Prometheus, Grafana)
 * <p>
 * Why This Pattern?
 * Micrometer Counters buffer metrics for time-series systems (typically 1-minute intervals).
 * This causes unacceptable delays for real-time dashboards. By maintaining LongAdders as the
 * source of truth and exposing them via FunctionCounters, we achieve:
 * - Immediate metric updates for operational dashboards
 * - Proper time-series formatting for monitoring systems
 * - Zero redundant storage (FunctionCounter reads directly from LongAdder)
 */
public class MicroMeterFlightRecorder implements FlightRecorder {

    private final MeterRegistry registry;
    private static final Auditor auditor = new Auditor(MarkerFactory.getMarker("flight"));
    private static final Logger logger = LoggerFactory.getLogger(MicroMeterFlightRecorder.class);

    /**
     * These LongAdders provide immediate, accurate counts for UI display
     * and are exposed to Micrometer via FunctionCounters.
     */

    // Cancellation metrics
    private final LongAdder cancelStatementCount = new LongAdder();
    private final LongAdder cancelPreparedStatementCount = new LongAdder();

    // Completion metrics
    private final LongAdder completedStatementCount = new LongAdder();
    private final LongAdder completedPreparedStatementCount = new LongAdder();

    // Byte transfer metrics - separate counters for each stream type
    private final LongAdder statementBytesOut = new LongAdder();
    private final LongAdder preparedStatementBytesOut = new LongAdder();

    // Lifecycle metrics
    private final LongAdder statementStartCount = new LongAdder();
    private final LongAdder preparedStatementStartCount = new LongAdder();
    private final LongAdder statementEndCount = new LongAdder();

    // Error metrics
    private final LongAdder statementErrorCount = new LongAdder();
    private final LongAdder preparedStatementErrorCount = new LongAdder();

    // Timeout metrics
    private final LongAdder timeoutStatementCount = new LongAdder();
    private final LongAdder timeoutPreparedStatementCount = new LongAdder();

    // Start time tracking
    private final AtomicLong startTime = new AtomicLong(0);

    /**
     * Creates a new MicroMeterFlightRecorder and registers all metrics with the provided registry.
     *
     * @param registry   the Micrometer registry for metric registration
     * @param producerId unique identifier for this producer instance (used as metric tag)
     */
    public MicroMeterFlightRecorder(MeterRegistry registry, String producerId) {
        this.registry = registry;
        this.startTime.set(System.currentTimeMillis());

        // Register all LongAdders as FunctionCounters.
        // This creates the bridge to Micrometer without duplicating data.

        registerAdder("cancel_statement", producerId, cancelStatementCount);
        registerAdder("cancel_prepared_statement", producerId, cancelPreparedStatementCount);

        registerAdder("stream_statement_completed", producerId, completedStatementCount);
        registerAdder("stream_prepared_statement_completed", producerId, completedPreparedStatementCount);

        registerAdder("stream_statement", producerId, statementStartCount);
        registerAdder("stream_prepared_statement", producerId, preparedStatementStartCount);
        registerAdder("statement_start", producerId, statementStartCount);
        registerAdder("statement_end", producerId, statementEndCount);

        registerAdder("stream_statement_error", producerId, statementErrorCount);
        registerAdder("stream_prepared_statement_error", producerId, preparedStatementErrorCount);
        registerAdder("statement_error", producerId, statementErrorCount);

        registerAdder("stream_statement_timeout", producerId, timeoutStatementCount);
        registerAdder("stream_prepared_statement_timeout", producerId, timeoutPreparedStatementCount);

        registerAdder("stream_statement_bytes_out", producerId, statementBytesOut);
        registerAdder("stream_prepared_statement_bytes_out", producerId, preparedStatementBytesOut);

        logger.info("MicroMeterFlightRecorder initialized for producer '{}' with {} real-time metrics", producerId, 16);
    }

    /**
     * Registers a LongAdder with Micrometer as a FunctionCounter.
     * <p>
     * The FunctionCounter reads directly from the LongAdder on each scrape,
     * ensuring monitoring systems see the same values as the UI.
     *
     * @param name       metric name suffix (prefixed with "dazzleduck.flight.")
     * @param producerId producer identifier for tagging
     * @param adder      the LongAdder to expose
     */
    private void registerAdder(String name, String producerId, LongAdder adder) {
        FunctionCounter.builder(
                        "dazzleduck.flight." + name + ".count",
                        adder,
                        LongAdder::sum
                )
                .tag("producer", producerId)
                .description("Real-time counter for " + name)
                .register(registry);
    }

    /**
     * Original counter method - kept for compatibility if needed.
     * Use registerAdder() for real-time metrics.
     */
    private Counter counter(String name, String producerId) {
        return Counter.builder("dazzleduck.flight." + name + ".count")
                .tag("producer", producerId)
                .register(registry);
    }

    // ---------------------------------------------------------------------------
    // Recording Methods - Statement Lifecycle with Audit Trail
    // ---------------------------------------------------------------------------

    @Override
    public void recordStatementCancel(CacheKey key, StatementContext<?> ctx) {
        cancelStatementCount.increment();
        auditor.audit(buildAudit(key, ctx, "CANCEL", null));
    }

    @Override
    public void recordPreparedStatementCancel(CacheKey key, StatementContext<?> ctx) {
        cancelPreparedStatementCount.increment();
        auditor.audit(buildAudit(key, ctx, "CANCEL", null));
    }

    @Override
    public void recordStatementStreamStart(CacheKey key, StatementContext<?> ctx) {
        statementStartCount.increment();
        auditor.audit(buildAudit(key, ctx, "START", null));
    }

    @Override
    public void recordStatementStreamEnd(CacheKey key, StatementContext<?> ctx) {
        statementEndCount.increment();
        auditor.audit(buildAudit(key, ctx, "END", null));
    }

    @Override
    public void recordStatementStreamError(CacheKey key, StatementContext<?> ctx, Throwable error) {
        statementErrorCount.increment();
        String errorMessage = error.getClass().getSimpleName() + ": " + error.getMessage();
        auditor.audit(buildAudit(key, ctx, "ERROR", errorMessage));
    }

    @Override
    public void recordStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        timeoutStatementCount.increment();
        auditor.audit(buildAudit(key, ctx, "TIMEOUT", null));
    }

    @Override
    public void recordPreparedStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        timeoutPreparedStatementCount.increment();
        auditor.audit(buildAudit(key, ctx, "TIMEOUT", null));
    }

    // ---------------------------------------------------------------------------
    // Recording Methods - Simple Counter Updates (No Audit)
    // ---------------------------------------------------------------------------

    @Override
    public void startStreamStatement() {
        statementStartCount.increment();
    }

    @Override
    public void endStreamStatement() {
        completedStatementCount.increment();
    }

    @Override
    public void errorStreamStatement() {
        statementErrorCount.increment();
    }

    @Override
    public void errorStreamPreparedStatement() {
        preparedStatementErrorCount.increment();
    }

    @Override
    public void startStreamPreparedStatement() {
        preparedStatementStartCount.increment();
    }

    @Override
    public void endStreamPreparedStatement() {
        completedPreparedStatementCount.increment();
    }

    @Override
    public void errorPreparedStreamStatement() {
        preparedStatementErrorCount.increment();
    }

    // ---------------------------------------------------------------------------
    // Recording Methods - Byte Transfer Tracking
    // ---------------------------------------------------------------------------

    @Override
    public void recordGetStreamPreparedStatement(long size) {
        if (size > 0) {
            preparedStatementBytesOut.add(size);
        }
    }

    @Override
    public void recordGetStreamStatement(long size) {
        if (size > 0) {
            statementBytesOut.add(size);
        }
    }

    // ---------------------------------------------------------------------------
    // Write Queue Registration
    // ---------------------------------------------------------------------------

    /**
     * Registers custom counters for a write/ingestion queue.
     *
     * This allows dynamic registration of queue-specific metrics that will be
     * exposed via Micrometer with the queue identifier as a tag.
     *
     * @param identifier unique identifier for the write queue
     * @param counters   map of counter names to their value suppliers
     */
    @Override
    public void registerWriteQueue(String identifier, Map<String, LongSupplier> counters) {
        if (counters == null || counters.isEmpty()) {
            logger.warn("Attempted to register write queue '{}' with null or empty counters", identifier);
            return;
        }

        counters.forEach((name, supplier) -> {
            FunctionCounter.builder("dazzleduck.flight.ingest_queue." + name, supplier, LongSupplier::getAsLong)
                    .tag("identifier", identifier)
                    .description("Write queue counter for " + identifier + "." + name)
                    .register(registry);
        });
        logger.debug("Registered write queue '{}' with {} counters", identifier, counters.size());
    }

    // ---------------------------------------------------------------------------
    // Public API - Real-Time Value Accessors
    //
    // These methods return the current sum from LongAdders, providing
    // immediate access to metric values without Micrometer buffering delay.
    // ---------------------------------------------------------------------------

    @Override
    public double getBytesOut() {
        return statementBytesOut.sum() + preparedStatementBytesOut.sum();
    }

    @Override
    public double getBytesIn() {
        // Not currently tracked
        return 0;
    }

    @Override
    public long getCompletedStatements() {
        return completedStatementCount.sum();
    }

    @Override
    public long getCompletedPreparedStatements() {
        return completedPreparedStatementCount.sum();
    }

    @Override
    public long getCancelledStatements() {
        return cancelStatementCount.sum();
    }

    @Override
    public long getCancelledPreparedStatements() {
        return cancelPreparedStatementCount.sum();
    }

    // ---------------------------------------------------------------------------
    // Additional Monitoring Accessors
    //
    // Extended API for detailed metric access beyond the FlightRecorder interface.
    // ---------------------------------------------------------------------------

    public long getStatementTimeouts() {
        return timeoutStatementCount.sum();
    }

    public long getPreparedStatementTimeouts() {
        return timeoutPreparedStatementCount.sum();
    }

    public long getStatementErrors() {
        return statementErrorCount.sum();
    }

    public long getPreparedStatementErrors() {
        return preparedStatementErrorCount.sum();
    }

    public long getStatementStarts() {
        return statementStartCount.sum();
    }

    public long getPreparedStatementStarts() {
        return preparedStatementStartCount.sum();
    }

    public double getStatementBytesOut() {
        return statementBytesOut.sum();
    }

    public double getPreparedStatementBytesOut() {
        return preparedStatementBytesOut.sum();
    }

    /**
     * Returns the Micrometer registry for advanced integration scenarios.
     *
     * @return the MeterRegistry instance used by this recorder
     */
    public MeterRegistry getRegistry() {
        return registry;
    }

    // ---------------------------------------------------------------------------
    // Helper Methods
    // ---------------------------------------------------------------------------

    /**
     * Builds a StatementAudit object for audit logging.
     *
     * @param key    cache key containing statement ID and peer identity
     * @param ctx    statement context with query and timing information
     * @param action the action being audited (START, END, CANCEL, ERROR, TIMEOUT)
     * @param error  error message if applicable, null otherwise
     * @return populated StatementAudit instance
     */
    private static StatementAudit buildAudit(CacheKey key, StatementContext<?> ctx,
                                             String action, String error) {
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