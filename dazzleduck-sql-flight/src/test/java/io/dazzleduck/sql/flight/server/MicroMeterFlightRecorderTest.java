package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer.CacheKey;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class MicroMeterFlightRecorderTest {

    private MeterRegistry registry;
    private MicroMeterFlightRecorder recorder;

    @BeforeEach
    void setup() {
        registry = new SimpleMeterRegistry();
        recorder = new MicroMeterFlightRecorder(registry, "producer1");
    }

    private Counter counter(String metric) {
        return registry.find("dazzleduck.flight." + metric + ".count")
                .tag("producer", "producer1")
                .counter();
    }

    @Test
    void testRecordStatementCancel() {
        recorder.recordStatementCancel(dummyKey(), dummyContext(false, "SELECT 1"));

        Counter c = counter("cancel_statement");
        assertNotNull(c);
        assertEquals(1.0, c.count());
    }

    @Test
    void testRecordPreparedStatementCancel() {
        recorder.recordPreparedStatementCancel(dummyKey(), dummyContext(true, "SELECT 1"));

        Counter c = counter("cancel_prepared_statement");
        assertNotNull(c);
        assertEquals(1.0, c.count());
    }

    @Test
    void testStartAndEndStreamStatement() {
        recorder.startStreamStatement();
        recorder.endStreamStatement();

        assertEquals(1.0, counter("stream_statement").count());
        assertEquals(1.0, counter("stream_statement_completed").count());
    }

    @Test
    void testStartAndEndPreparedStatement() {
        recorder.startStreamPreparedStatement();
        recorder.endStreamPreparedStatement();

        assertEquals(1.0, counter("stream_prepared_statement").count());
        assertEquals(1.0, counter("stream_prepared_statement_completed").count());
    }


    @Test
    void testRecordGetStreamPreparedStatementDoesNotThrow() {
        recorder.recordGetStreamPreparedStatement(123);
    }

    private static StatementContext<Statement> dummyContext(boolean prepared, String query) {
        Statement dummyStmt = (Statement) Proxy.newProxyInstance(MicroMeterFlightRecorderTest.class.getClassLoader(), new Class<?>[]{prepared ? PreparedStatement.class : Statement.class}, (proxy, method, args) -> 0);
        StatementContext<Statement> ctx = new StatementContext<>(ConnectionPool.getConnection(), dummyStmt, query);
        ctx.start();
        ctx.bytesOut(100);
        ctx.end();
        return ctx;
    }

    private static CacheKey dummyKey() {
        return new CacheKey("test-user", 42L);
    }
}
