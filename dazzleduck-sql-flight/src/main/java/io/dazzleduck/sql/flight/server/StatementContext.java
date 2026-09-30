/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dazzleduck.sql.flight.server;

import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.util.AutoCloseables;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Context for {@link T} to be persisted in memory in between {@link FlightSqlProducer} calls.
 *
 * @param <T> the {@link Statement} to be persisted.
 */
public final class StatementContext<T extends Statement> implements AutoCloseable {

    private final T statement;
    private final String query;
    private boolean inUse = false;
    private Instant startTime;
    private Instant endTime;
    private int useCount;
    // Set when the cursor cache evicts this context while a stream is using it (see closeWhenIdle).
    private boolean closeWhenDone;
    private boolean closed;
    // A stream has been handed this context (see markClaimed), and when it was last active.
    private boolean claimed;
    // Set by cancel(); a queued stream checks it after start() and ends without running the query.
    private boolean cancelRequested;
    private long lastActiveNanos = System.nanoTime();

    private long bytesOut;

    private final boolean isPreparedStatementContext;

    private final Connection connection;


    public StatementContext(final Connection connection, final T statement, final String query) {
        this.statement = Objects.requireNonNull(statement, "statement cannot be null.");
        this.query = query;
        this.connection = connection;
        this.isPreparedStatementContext = statement instanceof PreparedStatement;
    }

    /**
     * Gets the statement wrapped by this {@link StatementContext}.
     *
     * @return the inner statement.
     */
    public T getStatement() {
        return statement;
    }

    public boolean isPreparedStatementContext() {
        return isPreparedStatementContext;
    }
    /**
     * Gets the optional SQL query wrapped by this {@link StatementContext}.
     *
     * @return the SQL query if present; empty otherwise.
     */
    public String getQuery() {
        return query;
    }

    /**
     * Closes the statement and connection. Idempotent. Synchronized with {@link #cancel} so a
     * cancel arriving from another thread never touches a statement that is being closed.
     */
    @Override
    public synchronized void close()  {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if ( !statement.isClosed())
                statement.close();
            if ( !connection.isClosed()){
                connection.close();
            }

        } catch (Exception e ){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StatementContext)) {
            return false;
        }
        final StatementContext<?> that = (StatementContext<?>) other;
        return statement.equals(that.statement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statement);
    }


    public synchronized Instant startTime() {
        return startTime;
    }

    public synchronized Instant endTime() {
        return endTime;
    }

    public synchronized boolean running() {
        return startTime != null && endTime == null;
    }

    public synchronized void start() {
        if(inUse) {
            throw new IllegalStateException("Context already in use");
        }
        inUse = true;
        this.startTime = Clock.systemUTC().instant();
        this.endTime = null;
        useCount += 1;
    }

    public synchronized void end() {
        inUse = false;
        claimed = false;
        cancelRequested = false;
        lastActiveNanos = System.nanoTime();
        this.endTime = Clock.systemUTC().instant();
        if (closeWhenDone) {
            close();
        }
    }

    /**
     * Interrupts the query if it is executing or streaming; a no-op once closed. Safe to call from
     * any thread, e.g. a gRPC cancel handler or a Flight CancelFlightInfo call.
     */
    public synchronized void cancel() throws SQLException {
        if (closed) {
            return;
        }
        // Recorded only while a stream exists to see it: one queued (claimed) checks it right after
        // start() and ends instead of running the query. A late cancel, after the stream ended, must
        // not stick to a context that stays open (a prepared statement) and cancel its next run.
        if (claimed || inUse) {
            cancelRequested = true;
        }
        if (inUse) {
            statement.cancel();
        }
    }

    /** Whether {@link #cancel} was called since this context's last stream ended. */
    public synchronized boolean isCancelRequested() {
        return cancelRequested;
    }

    /**
     * Marks that a stream has been handed this context and will start it. Set before the stream
     * task is queued, so a query waiting for an executor thread counts as live just like a
     * running one; cleared when the stream {@link #end ends}.
     */
    public synchronized void markClaimed() {
        claimed = true;
    }

    /**
     * Whether nothing will use this context again unless a client comes back for it: no stream
     * has claimed it (or its stream has ended), it is not in use, and it has been idle for longer
     * than {@code ttl}. A queued or running query is never idle, however long it takes.
     */
    public synchronized boolean idleLongerThan(Duration ttl) {
        return !claimed && !inUse && System.nanoTime() - lastActiveNanos > ttl.toNanos();
    }

    /**
     * Closes the statement and connection now, or, if a stream is using them, when that stream
     * {@link #end ends}. For automatic cache evictions: the cursor TTL is meant to reap cursors that
     * were planned but never read, not to close a query that is still executing or streaming.
     */
    public synchronized void closeWhenIdle() {
        // Claimed covers a stream still queued for an executor thread: closing now would fail its
        // task with "statement closed" instead of letting it see the cancel and end cleanly.
        if (inUse || claimed) {
            closeWhenDone = true;
        } else {
            close();
        }
    }

    public synchronized void bytesOut(long out) {
        this.bytesOut +=out;
    }

    public synchronized long bytesOut() {
        return this.bytesOut;
    }

    public synchronized long useCount() {
        return useCount;
    }

    public String getDatabase() {
        try { return connection.getCatalog(); } catch (SQLException e) { return null; }
    }

    public String getSchema() {
        try { return connection.getSchema(); } catch (SQLException e) { return null; }
    }
}