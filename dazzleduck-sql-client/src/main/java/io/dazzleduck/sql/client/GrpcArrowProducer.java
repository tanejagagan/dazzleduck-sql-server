package io.dazzleduck.sql.client;



import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.client.auth.AuthUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public final class GrpcArrowProducer extends ArrowProducer.AbstractArrowProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GrpcArrowProducer.class);

    private final FlightSqlClient client;
    private final BufferAllocator allocator;
    private final FlightSqlClient.ExecuteIngestOptions ingestOptions;
    private final long maxMem;
    private final long maxDisk;
    private final Duration grpcTimeout;

    public GrpcArrowProducer(
            Schema schema,
            long minBatchSize,
            long maxBatchSize,
            Duration maxSendInterval,
            Clock clock,
            int retryCount,
            long retryIntervalMillis,
            java.util.List<String> projections,
            java.util.List<String> partitionBy,
            long maxInMemorySize,
            long maxOnDiskSize,
            BufferAllocator allocator,
            Location location,
            String username,
            String password,
            Map<String, String> ingestParams,
            Duration grpcTimeout
    ) {
        super(minBatchSize, maxBatchSize, maxSendInterval, schema, clock, retryCount, retryIntervalMillis, projections, partitionBy);

        // Validate parameters
        this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
        Objects.requireNonNull(location, "location must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(password, "password must not be null");
        Objects.requireNonNull(ingestParams, "ingestParams must not be null");
        Objects.requireNonNull(grpcTimeout, "grpcTimeout must not be null");

        if (username.trim().isEmpty()) {
            throw new IllegalArgumentException("username must not be empty");
        }
        if (grpcTimeout.isNegative() || grpcTimeout.isZero()) {
            throw new IllegalArgumentException("grpcTimeout must be positive");
        }

        this.maxMem = maxInMemorySize;
        this.maxDisk = maxOnDiskSize;
        this.grpcTimeout = grpcTimeout;

        logger.info("Initializing GrpcFlightSender with location={}, timeout={}",
                    location.getUri(), this.grpcTimeout);

        this.client = new FlightSqlClient(FlightClient.builder(allocator, location)
                        .intercept(AuthUtils.createClientMiddlewareFactory(
                                username,
                                password,
                                Map.of()
                        ))
                        .build()
        );

        // Add projections and partitionBy to ingestParams
        Map<String, String> enrichedParams = new java.util.HashMap<>(ingestParams);
        if (!getProjections().isEmpty()) {
            String projectionsValue = String.join(",", getProjections());
            enrichedParams.put(Headers.HEADER_DATA_PROJECTIONS, projectionsValue);
        }
        if (!getPartitionBy().isEmpty()) {
            String partitionByValue = String.join(",", getPartitionBy());
            enrichedParams.put(Headers.HEADER_DATA_PARTITION, partitionByValue);
        }

        this.ingestOptions = new FlightSqlClient.ExecuteIngestOptions("", FlightSql.CommandStatementIngest
                                .TableDefinitionOptions
                                .newBuilder()
                                .build(),
                        false,
                        "",
                        "",
                        enrichedParams
                );
    }

    @Override
    public long getMaxInMemorySize() {
        return maxMem;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxDisk;
    }

    @Override
    protected void doSend(ProducerElement element) throws InterruptedException {
        // Check if thread was interrupted before attempting send
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before send");
        }

        logger.debug("Sending element via gRPC");

        // Read the element and send it via gRPC
        // Use child allocator for better memory management
        try (org.apache.arrow.memory.BufferAllocator childAllocator =
                allocator.newChildAllocator("grpc-send", 0, Long.MAX_VALUE)) {

            try (java.io.InputStream in = element.read();
                 org.apache.arrow.vector.ipc.ArrowStreamReader reader =
                    new org.apache.arrow.vector.ipc.ArrowStreamReader(in, childAllocator)) {

                client.executeIngest(reader, ingestOptions);
                logger.info("Successfully sent element via gRPC");
            }
        } catch (Exception e) {
            // If interrupted during operation, throw InterruptedException instead
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted during gRPC send");
            }
            logger.error("gRPC ingestion failed for element", e);
            throw new RuntimeException("gRPC ingestion failed", e);
        }
    }

    @Override
    public void close() {
        Exception superCloseException = null;

        // Close parent resources first
        try {
            super.close();
        } catch (Exception e) {
            superCloseException = e;
            logger.error("Error closing FlightSender resources", e);
        }

        // Always attempt to close the client
        try {
            client.close();
            logger.debug("FlightSqlClient closed successfully");
        } catch (Exception e) {
            logger.error("Failed to close FlightSqlClient", e);
            // If super.close() also failed, add this as a suppressed exception
            if (superCloseException != null) {
                superCloseException.addSuppressed(e);
            }
        }

        // Rethrow the first exception if any occurred
        if (superCloseException != null) {
            throw (RuntimeException) superCloseException;
        }
    }
}
