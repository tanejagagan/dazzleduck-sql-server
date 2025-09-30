package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils {
    private final String warehousePath;
    private final Config config;

    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    private static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

    private static final String TEMP_LOCATION = "/tmp/dazzleduck-writes";

    private final ConcurrentHashMap<String, BulkIngestQueue<String, Result >> ingestionQueueMap =
            new ConcurrentHashMap<>();

    public IngestionService(String warehousePath, Config config) {
        this.warehousePath = warehousePath;
        this.config = config;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws ExecutionException, InterruptedException, IOException {
        var contentType = serverRequest.headers().value(HeaderNames.CONTENT_TYPE);
        if (contentType.isEmpty() || !contentType.get().equals(ContentTypes.APPLICATION_ARROW)) {
            serverResponse.status(Status.UNSUPPORTED_MEDIA_TYPE_415);
            serverResponse.send();
            return;
        }
        UriQuery query = serverRequest.query();
        var path = query.get("path");
        final String completePath = warehousePath + "/" + path;

        String format = ParameterUtils.getParameterValue(HEADER_DATA_FORMAT, serverRequest, "parquet", String.class);
        var partitions = ParameterUtils.getParameterValue(HEADER_DATA_PARTITION, serverRequest, null, String.class);
        var tranformationString = ParameterUtils.getParameterValue(HEADER_DATA_TRANSFORMATION, serverRequest, null, String.class);
        var producerId = ParameterUtils.getParameterValue(HEADER_PRODUCER_ID, serverRequest, null, String.class);
        var producerBatchId = ParameterUtils.getParameterValue(HEADER_PRODUCER_BATCH_ID, serverRequest, -1L, Long.class);
        var sortOrder = ParameterUtils.getParameterValue(HEADER_SORT_ORDER, serverRequest, null, String.class);
        int totalSize = 1024;
        // Ensure the temp directory exists
        Path tempDir = Paths.get(TEMP_LOCATION);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }

        // Create a unique temp file inside TEMP_LOCATION with UUID
        String uniqueFileName = "ingestion_" + UUID.randomUUID() + ".arrow";
        Path tempFilePath = tempDir.resolve(uniqueFileName);

        // Read request body and write it to temp file
        try (InputStream in = serverRequest.content().inputStream();
             OutputStream out = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            in.transferTo(out);
        }

        String tempFile = tempFilePath.toAbsolutePath().toString();
        var batch = new Batch<>(
                splitParam(sortOrder),
                splitParam(tranformationString),
                splitParam(partitions),
                tempFile,
                producerId,
                producerBatchId,
                totalSize,
                format,
                Instant.now()
        );
        var ingestionQueue = ingestionQueueMap.computeIfAbsent(completePath, p -> new ParquetIngestionQueue(p, p, DEFAULT_MAX_BUCKET_SIZE, DEFAULT_MAX_DELAY, Executors.newSingleThreadScheduledExecutor(), Clock.systemDefaultZone()));
        var result = ingestionQueue.addToQueue(batch);
        var futureResult = result.get(); // blocks until batch processed
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.valueToTree(futureResult);
        jsonNode.put("completionTime", Instant.now().toString());
        serverResponse.status(Status.OK_200).header(HeaderNames.CONTENT_TYPE, ContentTypes.APPLICATION_JSON).send(jsonNode.toString());
    }

    public record Result(String fileName) {
    }
    public static class ParquetIngestionQueue extends BulkIngestQueue<String, Result> {

        private final String path;
        /**
         * Thw write will be performed as soon as bucket is full or after the maxDelay is exported since the first batch is inserted
         *
         * @param identifier      identify the queue. Generally this will the path of the bucket
         * @param maxBucketSize   size of the bucket. Write will be performed as soon as bucket is full or overflowing
         * @param maxDelay        write will be performed just after this delay.
         * @param executorService Executor service.
         * @param clock
         */
        public ParquetIngestionQueue(String path, String identifier, long maxBucketSize, Duration maxDelay, ScheduledExecutorService executorService, Clock clock) {
            super(identifier, maxBucketSize, maxDelay, executorService, clock);
            this.path = path;
        }

        @Override
        protected void write(WriteTask<String, Result> writeTask) {
            var batches = writeTask.bucket().batches();
            // All Arrow files
            var arrowFiles = batches.stream().map(Batch::record).map("'%s'"::formatted).collect(Collectors.joining(","));
            // Last transformation
            var lastTransformation = batches.stream().map(Batch::transformations).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().toList().stream().reduce((a, b) -> b).orElse("");
            // Last sort order
            var lastSortOrder = batches.stream().map(Batch::sortOrder).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().reduce((a, b) -> b).map(s -> " ORDER BY " + s).orElse("");
            // Last partition
            var lastPartition = batches.stream().map(Batch::partitions).filter(Objects::nonNull).flatMap(Arrays::stream).map(String::trim).filter(s -> !s.isEmpty()).distinct().reduce((a, b) -> b).map(s -> ", PARTITION_BY (" + s + ")").orElse("");
            // Select clause
            var selectClause = lastTransformation.isEmpty() ? "*" : "*, " + lastTransformation;
            // Last format
            var format = batches.isEmpty() ? "" : batches.get(batches.size() - 1).format();
            // Build SQL
            var sql = """
                COPY
                    (SELECT %s FROM read_arrow([%s])%s)
                    TO '%s'
                    (FORMAT %s %s);
                """.formatted(selectClause, arrowFiles, lastSortOrder, this.path, format, lastPartition);
            ConnectionPool.execute(sql);
            writeTask.bucket().futures().forEach(action -> action.complete(new Result(this.path)));
        }
    }

    private static String[] splitParam(String value) {
        return (value == null || value.isBlank()) ? new String[0] : value.split(",");
    }
}
