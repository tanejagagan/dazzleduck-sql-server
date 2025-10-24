package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.commons.ingestion.Batch;

import java.time.Instant;
import java.util.Map;

public record IngestionParameters(String completePath, String format, String[] partitions, String[] transformations,
                                  String[] sortOrder, String producerId, Long producerBatchId,
                                  Map<String, String> parameters) {
    public Batch<String> constructBatch(long size, String tempFile) {
        return new Batch<>(
                sortOrder,
                transformations,
                partitions,
                tempFile,
                producerId,
                producerBatchId,
                size,
                format,
                Instant.now()
        );
    }
}
