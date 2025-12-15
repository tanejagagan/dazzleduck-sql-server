package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.model.FlightMetricsSnapshot;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.mockito.Mockito.*;

import java.util.concurrent.TimeUnit;

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
        StatementContext<?> ctx = mockContext(false);
        recorder.recordStatementCancel(ctx);

        Counter c = counter("cancel_statement");
        assertNotNull(c);
        assertEquals(1.0, c.count());
    }

    @Test
    void testRecordPreparedStatementCancel() {
        StatementContext<?> ctx = mockContext(true);
        recorder.recordPreparedStatementCancel(ctx);

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
    void testStartAndEndBulkIngest() {
        recorder.startBulkIngest();
        recorder.endBulkIngest();
        assertEquals(1.0, counter("bulk_ingest_completed").count());
    }

    @Test
    void testRecordGetStreamPreparedStatementDoesNotThrow() {
        recorder.recordGetStreamPreparedStatement(123);
    }


    private StatementContext<Statement> mockContext(boolean prepared) {
        Statement stmt = mock(Statement.class);
        StatementContext<Statement> ctx = new StatementContext<>(
                stmt,
                "SELECT 1",
                "test-user",
                42L
        );
        if (prepared) {
            PreparedStatement ps = mock(PreparedStatement.class);
            ctx = new StatementContext<>(ps, "SELECT 1", "test-user", 42L);
        }
        ctx.start();
        return ctx;
    }
}
