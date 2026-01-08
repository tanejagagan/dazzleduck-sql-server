package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ingestion.Batch;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

public record IngestionParameters(String path,
                                  String format, String[] partitions, String[] transformations,
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

    public String completePath(String warehousePath) {
        return Path.of(warehousePath).resolve(path).toString();
    }

    public static IngestionParameters getIngestionParameters(FlightSql.CommandStatementIngest command) {
        Map<String, String> optionMap = command.getOptionsMap();
        String path = optionMap.get(Headers.HEADER_PATH);
        String format = optionMap.getOrDefault(Headers.HEADER_DATA_FORMAT, "parquet");

        Function<String, String[]> splitCsv = value -> {
            if (value == null || value.isBlank()) return new String[0];
            return java.util.Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        };

        String producerId = optionMap.get(Headers.HEADER_PRODUCER_ID);
        // Optional comma-separated lists
        String[] partitions = splitCsv.apply(optionMap.get(Headers.HEADER_DATA_PARTITION));
        String[] transformations = splitCsv.apply(optionMap.get(Headers.HEADER_DATA_TRANSFORMATION));
        String[] sortOrder = splitCsv.apply(optionMap.get(Headers.HEADER_SORT_ORDER));
        return new IngestionParameters(path, format, partitions, transformations, sortOrder, producerId, 0L, Map.of());
    }
}
