package io.dazzleduck.sql.commons.ingestion;

import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

public record Batch<T>(String[] sortOrder,
                       String[] transformations,
                       String[] partitions,
                       T record,
                       String producerId,
                       long producerBatchId,
                       long totalSize,
                       String format,
                       Instant receivedTime) {
}
