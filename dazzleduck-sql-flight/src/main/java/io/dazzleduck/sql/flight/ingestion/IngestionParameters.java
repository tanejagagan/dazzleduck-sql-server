package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.commons.ingestion.Batch;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public record IngestionParameters(String path,
                                  String format, String[] partitions, String[] transformations,
                                  String[] sortOrder, String producerId, Long producerBatchId,
                                  Map<String, String> parameters) {
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    public static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

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

    public FlightSql.CommandStatementIngest createCommand() {
        var options = Map.of("path", path(), "partitions",  String.join(",", partitions), "format", format());
        return FlightSql.CommandStatementIngest.newBuilder().putAllOptions(options).build();
    }

    public String completePath(String warehousePath){
        return URI.create(warehousePath).resolve(URI.create(path)).getPath();
    }
    public static IngestionParameters getIngestionParameters(FlightSql.CommandStatementIngest command){
        Map<String, String > optionMap = command.getOptionsMap();
        String path = optionMap.get("path");
        String format = optionMap.getOrDefault("format", "parquet");
        String partitionColumnString = optionMap.get("partitions");
        String[] partitionColumns = new String[0];
        if(partitionColumnString != null) {
            partitionColumns = partitionColumnString.split(",");
        }
        return new IngestionParameters(path, format, partitionColumns, new String[0], new String[0], "", 0L, Map.of());
    }
}
