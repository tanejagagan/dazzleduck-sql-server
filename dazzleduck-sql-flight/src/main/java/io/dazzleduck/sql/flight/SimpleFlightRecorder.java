package io.dazzleduck.sql.flight;

import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer.CacheKey;
import io.dazzleduck.sql.flight.server.StatementContext;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * A simple in-memory implementation of {@link FlightRecorder} that tracks metrics
 * using {@link LongAdder} counters. This implementation is thread-safe and provides
 * higher throughput under contention compared to AtomicLong, making it suitable for
 * high-concurrency metrics collection without external dependencies like Micrometer.
 */
public class SimpleFlightRecorder implements FlightRecorder {

    // Statement counters
    private final LongAdder statementStartCount = new LongAdder();
    private final LongAdder statementCancelCount = new LongAdder();
    private final LongAdder statementTimeoutCount = new LongAdder();
    private final LongAdder statementCompletedCount = new LongAdder();
    private final LongAdder statementErrorCount = new LongAdder();

    // Prepared statement counters
    private final LongAdder preparedStatementStartCount = new LongAdder();
    private final LongAdder preparedStatementCancelCount = new LongAdder();
    private final LongAdder preparedStatementTimeoutCount = new LongAdder();
    private final LongAdder preparedStatementCompletedCount = new LongAdder();
    private final LongAdder preparedStatementErrorCount = new LongAdder();

    // Stream lifecycle counters
    private final LongAdder streamStatementStartCount = new LongAdder();
    private final LongAdder streamStatementEndCount = new LongAdder();
    private final LongAdder streamStatementErrorCount = new LongAdder();

    // Byte counters
    private final LongAdder bytesOut = new LongAdder();

    @Override
    public void recordStatementCancel(CacheKey key, StatementContext<?> ctx) {
        statementCancelCount.increment();
    }

    @Override
    public void recordPreparedStatementCancel(CacheKey key, StatementContext<?> ctx) {
        preparedStatementCancelCount.increment();
    }

    @Override
    public void recordStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        statementTimeoutCount.increment();
    }

    @Override
    public void recordPreparedStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        preparedStatementTimeoutCount.increment();
    }

    @Override
    public void recordStatementStreamStart(CacheKey key, StatementContext<?> ctx) {
        streamStatementStartCount.increment();
    }

    @Override
    public void recordStatementStreamEnd(CacheKey key, StatementContext<?> ctx) {
        streamStatementEndCount.increment();
    }

    @Override
    public void recordStatementStreamError(CacheKey key, StatementContext<?> ctx, Throwable error) {
        streamStatementErrorCount.increment();
    }

    @Override
    public void startStreamStatement() {
        statementStartCount.increment();
    }

    @Override
    public void endStreamStatement() {
        statementCompletedCount.increment();
    }

    @Override
    public void errorStreamStatement() {
        statementErrorCount.increment();
    }

    @Override
    public void startStreamPreparedStatement() {
        preparedStatementStartCount.increment();
    }

    @Override
    public void endStreamPreparedStatement() {
        preparedStatementCompletedCount.increment();
    }

    @Override
    public void errorStreamPreparedStatement() {
        preparedStatementErrorCount.increment();
    }

    @Override
    public void errorPreparedStreamStatement() {
        // This method appears to be a duplicate of errorStreamPreparedStatement
        // Keeping for interface compatibility but tracking separately if needed
        preparedStatementErrorCount.increment();
    }

    @Override
    public void recordGetStreamPreparedStatement(long size) {
        if (size > 0) {
            bytesOut.add(size);
        }
    }

    @Override
    public void recordGetStreamStatement(long size) {
        if (size > 0) {
            bytesOut.add(size);
        }
    }

    @Override
    public void registerWriteQueue(String identifier, Map<String, LongSupplier> counters) {
        // No-op: This simple implementation does not support write queue registration.
        // Use MicroMeterFlightRecorder for full metrics integration.
    }

    @Override
    public double getBytesOut() {
        return bytesOut.sum();
    }

    @Override
    public double getBytesIn() {
        // Bytes in is not currently tracked in the Flight protocol implementation
        return 0;
    }

    @Override
    public long getCompletedStatements() {
        return statementCompletedCount.sum();
    }

    @Override
    public long getCancelledStatements() {
        return statementCancelCount.sum();
    }

    @Override
    public long getCompletedPreparedStatements() {
        return preparedStatementCompletedCount.sum();
    }

    @Override
    public long getCancelledPreparedStatements() {
        return preparedStatementCancelCount.sum();
    }
}
