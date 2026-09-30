package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The producer itself enforces write access on bulk ingest, for the HTTP entry point too, not only
 * the transport in front of it (HTTP's JWT filter also checks, so this calls the producer directly).
 */
class IngestWriteAccessTest {

    private static DuckDBFlightSqlProducer producer(AccessMode mode) {
        return new DuckDBFlightSqlProducer(
                FlightTestUtils.findNextLocation(), UUID.randomUUID().toString(), "secret", new RootAllocator(),
                System.getProperty("java.io.tmpdir"), mode, DuckDBFlightSqlProducer.newTempDir(), null,
                Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(1), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of());
    }

    private static final class Ack implements FlightProducer.StreamListener<PutResult> {
        Throwable error;

        @Override public void onNext(PutResult val) { }
        @Override public void onError(Throwable t) { error = t; }
        @Override public void onCompleted() { }
    }

    private static Ack ingestOverHttpEntryPoint(AccessMode mode) {
        var ack = new Ack();
        var context = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
        var params = new IngestionParameters("some_queue", "parquet", new String[0], new String[0], "p", 1L, Map.of());
        try {
            producer(mode).acceptPutStatementBulkIngest(context, params, new ByteArrayInputStream(new byte[0]), ack);
        } catch (RuntimeException ignored) {
            // COMPLETE gets past the check and then fails for other reasons (no queue configured)
        }
        return ack;
    }

    private static boolean refusedForWriteAccess(Ack ack) {
        return ack.error instanceof FlightRuntimeException f
                && f.status().code() == FlightStatusCode.UNAUTHORIZED
                && f.getMessage().contains("No write access");
    }

    @Test
    void readOnlyModesRefuseIngestAtTheProducer() {
        assertTrue(refusedForWriteAccess(ingestOverHttpEntryPoint(AccessMode.READ_ONLY)));
        assertTrue(refusedForWriteAccess(ingestOverHttpEntryPoint(AccessMode.RESTRICT_READ_ONLY)));
    }

    @Test
    void completeModeIsNotRefused() {
        assertFalse(refusedForWriteAccess(ingestOverHttpEntryPoint(AccessMode.COMPLETE)));
    }
}
