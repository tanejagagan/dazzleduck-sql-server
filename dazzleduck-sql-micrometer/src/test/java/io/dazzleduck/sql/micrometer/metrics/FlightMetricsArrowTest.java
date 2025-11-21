package io.dazzleduck.sql.micrometer.metrics;

import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class FlightMetricsArrowTest {

    private MeterRegistry registry;
    private FlightMetrics metrics;
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
                return Duration.ofSeconds(1);   // REQUIRED for StepMeterRegistry
            }
        };

        registry = new ArrowMicroMeterRegistry.Builder()
                .config(config)
                .endpoint("http://localhost:0")
                .clock(testClock)
                .testMode(true)                // no HTTP posting
                .build();

        metrics = new FlightMetrics(registry, "producer1");
    }

    // -------------------
    // Helper methods
    // -------------------
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

    private void advanceStep() {
        testClock.add(Duration.ofSeconds(1));
    }

    // -------------------------------------------------------------
    // Test: Callable with counter + timer
    // -------------------------------------------------------------
    @Test
    void testRecordGetFlightInfo() throws Exception {
        String val = metrics.recordGetFlightInfo(() -> "ok");

        advanceStep();  // REQUIRED for StepMeterRegistry

        assertEquals("ok", val);
        assertEquals(1.0, counter("get_flight_info").count());
        assertEquals(1, timer("get_flight_info").count());
    }

    // -------------------------------------------------------------
    // Test: Runnable timer
    // -------------------------------------------------------------
    @Test
    void testRecordBulkIngest() {
        metrics.recordBulkIngest(() -> {});

        advanceStep();

        assertEquals(1.0, counter("bulk_ingest").count());
        assertEquals(1, timer("bulk_ingest").count());
    }

    @Test
    void testTimerMeasuresTime() throws Exception {
        metrics.recordGetFlightInfo(() -> {
            Thread.sleep(25);
            return null;
        });

        advanceStep();
        Timer t = timer("get_flight_info");
        assertEquals(1, t.count());
        assertTrue(t.totalTime(TimeUnit.MILLISECONDS) <= 25);
    }

    @Test
    void testIncrementPreparedStatementUpdate() {
        metrics.incrementPreparedStatementUpdate();
        metrics.incrementPreparedStatementUpdate();

        advanceStep();

        assertEquals(2.0, counter("put_prepared_update").count());
    }
}
