package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.flight.FlightRecorder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Flight SQL producer for RESTRICT_READ_ONLY mode.
 *
 * Inherits from {@link SelectOnlyFlightSqlProducer}:
 * - SELECT-only enforcement via {@code transformQuery} and {@code transformPreparedStatementQuery}
 * - Per-table CTE filter injection (via {@code RestrictedReadOnlyAuthorizer})
 * - {@code acceptPutPreparedStatementUpdate} blocked (DML via prepared statement)
 *
 * Additionally blocks {@code getSchemaStatement} (raw-SQL schema probe that bypasses
 * the prepared-statement authorization path).
 */
public class RestrictedReadOnlyFlightSqlProducer extends SelectOnlyFlightSqlProducer {

    public RestrictedReadOnlyFlightSqlProducer(
            Location serverLocation, String producerId, String secretKey,
            BufferAllocator allocator, String warehousePath, AccessMode accessMode,
            Path tempDir, IngestionHandler postIngestionHandler,
            ScheduledExecutorService scheduledExecutorService,
            Duration queryTimeout, Duration maxQueryTimeout,
            Clock clock, FlightRecorder recorder,
            IngestionConfig ingestionConfig, List<Location> dataProcessorLocations) {
        this(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode,
              tempDir, postIngestionHandler, scheduledExecutorService,
              queryTimeout, maxQueryTimeout, clock, recorder, ingestionConfig, dataProcessorLocations, CursorConfig.DEFAULT);
    }

    public RestrictedReadOnlyFlightSqlProducer(
            Location serverLocation, String producerId, String secretKey,
            BufferAllocator allocator, String warehousePath, AccessMode accessMode,
            Path tempDir, IngestionHandler postIngestionHandler,
            ScheduledExecutorService scheduledExecutorService,
            Duration queryTimeout, Duration maxQueryTimeout,
            Clock clock, FlightRecorder recorder,
            IngestionConfig ingestionConfig, List<Location> dataProcessorLocations, CursorConfig cursorConfig) {
        super(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode,
              tempDir, postIngestionHandler, scheduledExecutorService,
              queryTimeout, maxQueryTimeout, clock, recorder, ingestionConfig, dataProcessorLocations, cursorConfig);
    }

    // ── Block raw-SQL schema probe (prepared-statement entry points are allowed;
    //    filter injection happens at createPreparedStatement via transformPreparedStatementQuery,
    //    and DML is blocked by the inherited acceptPutPreparedStatementUpdate override) ──────────

    @Override
    public final SchemaResult getSchemaStatement(
            FlightSql.CommandStatementQuery command,
            CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("getSchemaStatement");
    }
}
