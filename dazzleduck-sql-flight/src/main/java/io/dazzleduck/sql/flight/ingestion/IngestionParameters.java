package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ingestion.Batch;
import io.dazzleduck.sql.commons.util.HeaderUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.time.Instant;
import java.util.Map;

public record IngestionParameters(String ingestionQueue,
                                  String format, String[] partitionBy, String[] projections,
                                  String[] sortOrder, String producerId, Long producerBatchId,
                                  Map<String, String> parameters) {
    public Batch<String> constructBatch(long size, String tempFile) {
        return new Batch<>(
                sortOrder,
                projections,
                partitionBy,
                tempFile,
                producerId,
                producerBatchId,
                size,
                format,
                Instant.now()
        );
    }


    public static IngestionParameters getIngestionParameters(FlightSql.CommandStatementIngest command) {

        Map<String, String> optionMap = command.getOptionsMap();
        String ingestionQueue = optionMap.get(Headers.QUERY_PARAMETER_INGESTION_QUEUE);
        // Validate ingestion_queue to prevent traversal attacks
        if (ingestionQueue == null || ingestionQueue.isEmpty()) {
            throw new IllegalArgumentException("ingestion_queue parameter is required");
        }
        if (ingestionQueue.contains("..") || ingestionQueue.startsWith("/")) {
            throw new IllegalArgumentException("Invalid ingestion_queue: traversal not allowed");
        }

        String format = optionMap.getOrDefault(Headers.HEADER_DATA_FORMAT, "parquet");

        String producerId = optionMap.get(Headers.HEADER_PRODUCER_ID);
        // Optional comma-separated lists
        String[] partitionBy = HeaderUtils.parseCsv(optionMap.get(Headers.HEADER_DATA_PARTITION));
        String[] projections = HeaderUtils.parseCsv(optionMap.get(Headers.HEADER_DATA_PROJECT));
        String[] sortOrder = HeaderUtils.parseCsv(optionMap.get(Headers.HEADER_SORT_ORDER));
        return new IngestionParameters(ingestionQueue, format, partitionBy, projections, sortOrder, producerId, 0L, Map.of());
    }

    public FlightSql.CommandStatementIngest createCommand() {
        var options = Map.of(
                Headers.QUERY_PARAMETER_INGESTION_QUEUE, ingestionQueue(),
                Headers.HEADER_DATA_PARTITION, String.join(",", partitionBy()),
                Headers.HEADER_DATA_FORMAT, format(),
                Headers.HEADER_DATA_PROJECT, String.join(",", projections()),
                Headers.HEADER_SORT_ORDER, String.join(",", sortOrder()));
        return FlightSql.CommandStatementIngest.newBuilder().putAllOptions(options).build();
    }
}
