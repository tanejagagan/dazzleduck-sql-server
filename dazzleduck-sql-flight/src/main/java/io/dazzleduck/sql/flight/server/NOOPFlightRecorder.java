package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer.CacheKey;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public class NOOPFlightRecorder implements FlightRecorder {

    // Atomic counters to track metrics in a thread-safe manner
    private final AtomicLong statementCancelCount = new AtomicLong();
    private final AtomicLong preparedStatementCancelCount = new AtomicLong();
    private final AtomicLong statementTimeoutCount = new AtomicLong();
    private final AtomicLong preparedStatementTimeoutCount = new AtomicLong();
    private final AtomicLong completedStatements = new AtomicLong();
    private final AtomicLong failedStatements = new AtomicLong();
    private final AtomicLong completedPreparedStatements = new AtomicLong();
    private final AtomicLong failedPreparedStatements = new AtomicLong();
    private final AtomicLong completedBulkIngest = new AtomicLong();
    private final AtomicLong failedBulkIngest = new AtomicLong();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();
    private final AtomicLong statementStart = new AtomicLong();
    private final AtomicLong statementEnd = new AtomicLong();
    private final AtomicLong statementError = new AtomicLong();

    @Override
    public void recordStatementCancel(CacheKey key, StatementContext<?> ctx) {
        statementCancelCount.incrementAndGet();
    }

    @Override
    public void recordPreparedStatementCancel(CacheKey key, StatementContext<?> ctx) {
        preparedStatementCancelCount.incrementAndGet();
    }

    @Override
    public void recordStatementStreamStart(CacheKey key, StatementContext<?> ctx) {
        statementStart.incrementAndGet();
    }

    @Override
    public void recordStatementStreamEnd(CacheKey key, StatementContext<?> ctx) {
        statementEnd.incrementAndGet();
    }

    @Override
    public void recordStatementStreamError(CacheKey key, StatementContext<?> ctx, Throwable error) {
        statementError.incrementAndGet();
    }

    @Override
    public void recordStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        statementTimeoutCount.incrementAndGet();
    }

    @Override
    public void recordPreparedStatementTimeout(CacheKey key, StatementContext<?> ctx) {
        preparedStatementTimeoutCount.incrementAndGet();
    }

    @Override
    public void startStreamStatement() {
        // Logic for when a stream statement starts (e.g., increment active count)
    }

    @Override
    public void endStreamStatement() {
        completedStatements.incrementAndGet();
    }

    @Override
    public void errorStreamStatement() {
        failedStatements.incrementAndGet();
    }

    @Override
    public void errorStreamPreparedStatement() {
        failedPreparedStatements.incrementAndGet();
    }

    @Override
    public void startStreamPreparedStatement() {
        // Logic for when a prepared stream starts
    }

    @Override
    public void endStreamPreparedStatement() {
        completedPreparedStatements.incrementAndGet();
    }

    @Override
    public void errorPreparedStreamStatement() {
        // Assuming this tracks the same category of error as errorStreamPreparedStatement
        failedPreparedStatements.incrementAndGet();
    }


    @Override
    public void recordGetStreamPreparedStatement(long networkSize) {
        if (networkSize > 0) {
            bytesIn.addAndGet(networkSize);
        }
    }

    @Override
    public void recordGetStreamStatement(long size) {
        if (size > 0) {
            bytesIn.addAndGet(size);
        }
    }

    @Override
    public void registerWriteQueue(String identifier, Map<String, LongSupplier> counters) {

    }

    @Override
    public double getBytesOut() {
        return bytesOut.get();
    }

    @Override
    public double getBytesIn() {
        return bytesIn.get();
    }

    @Override
    public long getCompletedStatements() {
        return completedStatements.get();
    }

    @Override
    public long getCancelledPreparedStatements() {
        return preparedStatementCancelCount.get();
    }

    @Override
    public long getCancelledStatements() {
        return statementCancelCount.get();
    }

    @Override
    public long getCompletedPreparedStatements() {
        return completedPreparedStatements.get();
    }

}