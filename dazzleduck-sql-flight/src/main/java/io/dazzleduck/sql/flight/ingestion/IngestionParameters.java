package io.dazzleduck.sql.flight.ingestion;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ingestion.Batch;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record IngestionParameters(String path,
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

    public String completePath(String warehousePath) {
        return Path.of(warehousePath).resolve(path).toString();
    }

    public static IngestionParameters getIngestionParameters(FlightSql.CommandStatementIngest command) {

        Map<String, String> optionMap = command.getOptionsMap();
        String path = optionMap.get(Headers.HEADER_PATH);
        // Validate path to prevent path traversal attacks
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path parameter is required");
        }
        if (path.contains("..") || path.startsWith("/")) {
            throw new IllegalArgumentException("Invalid path: path traversal not allowed");
        }

        String format = optionMap.getOrDefault(Headers.HEADER_DATA_FORMAT, "parquet");

        String producerId = optionMap.get(Headers.HEADER_PRODUCER_ID);
        // Optional comma-separated lists
        String[] partitionBy = parseCsv(optionMap.get(Headers.HEADER_DATA_PARTITION));
        String[] projections = parseCsv(optionMap.get(Headers.HEADER_DATA_PROJECT));
        String[] sortOrder = parseCsv(optionMap.get(Headers.HEADER_SORT_ORDER));
        return new IngestionParameters(path, format, partitionBy, projections, sortOrder, producerId, 0L, Map.of());
    }

    static String[] parseCsv(String value) {
        if (value == null || value.isBlank()) {
            return new String[0];
        }
        List<String> result = new ArrayList<>();
        try (CsvReader<CsvRecord> reader = CsvReader.builder().ofCsvRecord(new StringReader(value))) {
            for (CsvRecord record : reader) {
                for (int i = 0; i < record.getFieldCount(); i++) {
                    String field = record.getField(i).trim();
                    if (!field.isEmpty()) {
                        result.add(field);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse CSV value: " + value, e);
        }
        return result.toArray(new String[0]);
    }

    public FlightSql.CommandStatementIngest createCommand() {
        var options = Map.of(
                Headers.HEADER_PATH, path(),
                Headers.HEADER_DATA_PARTITION, String.join(",", partitionBy()),
                Headers.HEADER_DATA_FORMAT, format(),
                Headers.HEADER_DATA_PROJECT, String.join(",", projections()),
                Headers.HEADER_SORT_ORDER, String.join(",", sortOrder()));
        return FlightSql.CommandStatementIngest.newBuilder().putAllOptions(options).build();
    }
}
