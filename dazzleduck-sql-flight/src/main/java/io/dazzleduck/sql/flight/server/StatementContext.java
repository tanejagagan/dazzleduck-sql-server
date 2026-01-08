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
import java.sql.Statement;
import java.time.Clock;
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

    @Override
    public void close()  {
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
        this.endTime = Clock.systemUTC().instant();
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
}