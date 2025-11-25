package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class MicroMeterFlightRecorderTest {

    private MeterRegistry registry;
    private MicroMeterFlightRecorder recorder;
    private MockClock testClock;

    @BeforeEach
    void setup() {
        testClock = new MockClock();

        ArrowRegistryConfig config = new ArrowRegistryConfig() {
            @Override
            public String get(String k) {
                if ("arrow.enabled".equals(k)) return "true";
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(1);  // REQUIRED for StepMeterRegistry
            }
        };

        registry = new ArrowMicroMeterRegistry.Builder()
                .config(config)
                .endpoint("http://localhost:0")
                .clock(testClock)
                .testMode(true)
                .build();

        recorder = new MicroMeterFlightRecorder(registry, "producer1");
    }

    private Counter counter(String name) {
        return registry.find("dazzleduck.flight." + name + ".count")
                .tag("producer", "producer1")
                .counter();
    }

    private Timer timer(String name) {
        return registry.find("dazzleduck.flight." + name + ".timer")
                .tag("producer", "producer1")
                .timer();
    }

    private Gauge gauge(String name) {
        return registry.find("dazzleduck.flight." + name)
                .tag("producer", "producer1")
                .gauge();
    }

    private void advanceStep() {
        testClock.add(Duration.ofSeconds(1)); // forces step publishing
    }

    @Test
    void testStartTimeGaugeExists() {
        Gauge g = gauge("start_time_ms");
        assertNotNull(g);
        assertTrue(g.value() > 0);
    }

    @Test
    void testRecordGetFlightInfo() throws Exception {
        String val = recorder.recordGetFlightInfo(() -> "ok");

        advanceStep();

        assertEquals("ok", val);
        assertEquals(1.0, counter("get_flight_info").count());
        assertEquals(1, timer("get_flight_info").count());
    }

    // -------------------------------------------------------------
    // Test: recordGetFlightInfoPrepared
    // -------------------------------------------------------------
    @Test
    void testRecordGetFlightInfoPrepared() throws Exception {
        int result = recorder.recordGetFlightInfoPrepared(() -> 42);

        advanceStep();

        assertEquals(42, result);
        assertEquals(1.0, counter("get_flight_info_prepared").count());
        assertEquals(1, timer("get_flight_info_prepared").count());
    }

    @Test
    void testRecordGetStreamStatement() {
        recorder.recordGetStreamStatement(() -> {});

        advanceStep();

        assertEquals(1.0, counter("get_stream_statement").count());
        assertEquals(1, timer("get_stream_statement").count());
        assertEquals(0.0, gauge("running_statements").value());
    }

    @Test
    void testRecordGetStreamStatementGaugeIncrement() {
        AtomicInteger before = new AtomicInteger((int) gauge("running_statements").value());

        recorder.recordGetStreamStatement(() -> {
            assertEquals(before.get() + 1, (int) gauge("running_statements").value());
        });

        advanceStep();

        assertEquals(before.get(), (int) gauge("running_statements").value());
    }

    @Test
    void testRecordGetStreamPreparedStatement() {
        recorder.recordGetStreamPreparedStatement(() -> {});

        advanceStep();

        assertEquals(1.0, counter("get_stream_prepared_statement").count());
        assertEquals(1, timer("get_stream_prepared_statement").count());
        assertEquals(0.0, gauge("running_prepared_statements").value());
    }

    @Test
    void testPreparedGaugeIncrement() {
        AtomicInteger before = new AtomicInteger((int) gauge("running_prepared_statements").value());

        recorder.recordGetStreamPreparedStatement(() -> {
            assertEquals(before.get() + 1, (int) gauge("running_prepared_statements").value());
        });

        advanceStep();

        assertEquals(before.get(), (int) gauge("running_prepared_statements").value());
    }


    @Test
    void testRecordBulkIngest() {
        recorder.recordBulkIngest(() -> {});

        advanceStep();

        assertEquals(1.0, counter("bulk_ingest").count());
        assertEquals(1, timer("bulk_ingest").count());
        assertEquals(0.0, gauge("running_bulk_ingest").value());
    }

    @Test
    void testRunningBulkIngestGaugeIncrement() {
        AtomicInteger before = new AtomicInteger((int) gauge("running_bulk_ingest").value());

        recorder.recordBulkIngest(() -> {
            assertEquals(before.get() + 1, (int) gauge("running_bulk_ingest").value());
        });

        advanceStep();

        assertEquals(before.get(), (int) gauge("running_bulk_ingest").value());
    }

    @Test
    void testRecordStatementCancel() {
        recorder.recordStatementCancel();

        advanceStep();

        assertEquals(1.0, counter("cancel_Statement_request").count());
    }

    @Test
    void testRecordPreparedStatementCancel() {
        recorder.recordPreparedStatementCancel();

        advanceStep();

        assertEquals(1.0, counter("cancel_Statement_Prepared_request").count());
    }

    @Test
    void testTimerMeasuresTime() {
        recorder.recordGetStreamPreparedStatement(() -> {
            try {
                Thread.sleep(10);
            } catch (Exception ignored) {}
        });

        advanceStep();
        Timer t = timer("get_stream_prepared_statement");

        assertEquals(1, t.count());
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) >= 10);
    }
}
