package io.dazzleduck.sql.http.server;

import java.time.Instant;

public record Batch<T>(String[] sortOrder,
                       String[] transformations,
                       T[] recordBatches,
                       String producerId,
                       long producerBatchId,
                       long totalSize,
                       Instant receivedTime) { }
