package io.dazzleduck.sql.http.server;

import io.helidon.common.uri.UriQuery;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import org.apache.arrow.memory.BufferAllocator;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.dazzleduck.sql.common.Headers.*;

public class IngestionService implements HttpService, ParameterUtils{
    private final String warehousePath;
    private final BufferAllocator allocator;

    private static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    private static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

    private static final String TEMP_LOCATION = "/tmp/dazzleduck-writes";

    private final ConcurrentHashMap<String, BulkIngestQueue<String, Result >> ingestionQueueMap =
            new ConcurrentHashMap<>();

    public IngestionService(String warehousePath, BufferAllocator allocator) {
        this.warehousePath = warehousePath;
        this.allocator = allocator;
    }

    @Override
    public void routing(HttpRules rules) {
        rules.post("/", this::handlePost);
    }

    private void handlePost(ServerRequest serverRequest, ServerResponse serverResponse) throws SQLException {
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
        List<String> partitionList = partitions == null ? List.of() : Arrays.asList(partitions.split(","));
        String producerId = null;
        long producerBatchId = -1L;
        int totalSize = 1024;
        String tempFile = null;

        // Write a temp file and write it at temp file location.
        //
        //var reader = new ArrowStreamReader(serverRequest.content().inputStream(), allocator);
        //ConnectionPool.bulkIngestToFile(reader, allocator, completePath, partitionList, format, tranformationString);

        var batch = new Batch<String>(new String[0],
                tranformationString.split(","),
                tempFile,
                null,
                producerBatchId,
                totalSize,
                format,
                Instant.now());
        var ingestionQueue = ingestionQueueMap.computeIfAbsent(completePath, p -> new ParquetIngestionQueue(p, p, DEFAULT_MAX_BUCKET_SIZE, DEFAULT_MAX_DELAY, Executors.newSingleThreadScheduledExecutor(), Clock.systemDefaultZone()));
        var result = ingestionQueue.addToQueue(batch);
        // Serialize the result
        //serverResponse.status(Status.OK_200);
        //serverResponse.send();
    }

    public static class Result {

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
            // Provide the  implementation
            // Read the files and write to destination
        }
    }
}
