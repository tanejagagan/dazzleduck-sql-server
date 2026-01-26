package io.dazzleduck.sql.client;

import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.common.types.VectorSchemaRootWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Channels;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public interface ArrowProducer extends Closeable {

    void close();

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void enqueue(byte[] input);

    /**
     * Creates combined Arrow stream bytes from a list of SendElements.
     * This utility method reads all Arrow batches from the input elements and combines them
     * into a single Arrow stream byte array.
     *
     * @param elements List of SendElements to combine
     * @param schema The Arrow schema for the data (used only if elements list is empty)
     * @param allocator Buffer allocator for Arrow operations
     * @return byte array containing the combined Arrow stream
     * @throws IOException if reading or writing Arrow data fails
     */
    static ProducerElement createCombinedReader(
            List<ProducerElement> elements,
            Schema schema,
            BufferAllocator allocator) throws IOException {
        return createCombinedReader(elements, schema, allocator, CompressionUtil.CodecType.NO_COMPRESSION);
    }

    /**
     * Creates combined Arrow stream bytes from a list of SendElements with optional compression.
     * This utility method reads all Arrow batches from the input elements and combines them
     * into a single Arrow stream byte array.
     *
     * @param elements List of SendElements to combine
     * @param schema The Arrow schema for the data (used only if elements list is empty)
     * @param allocator Buffer allocator for Arrow operations
     * @param compressionType The compression type to use for the output stream
     * @return byte array containing the combined Arrow stream
     * @throws IOException if reading or writing Arrow data fails
     */
    static ProducerElement createCombinedReader(
            List<ProducerElement> elements,
            Schema schema,
            BufferAllocator allocator,
            CompressionUtil.CodecType compressionType) throws IOException {

        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Cannot create combined reader from empty list");
        }

        // If only one element, return it directly
        if (elements.size() == 1) {
            return elements.get(0);
        }

        // Calculate min and max batch IDs from all elements
        long minBatchId = Long.MAX_VALUE;
        long maxBatchId = Long.MIN_VALUE;
        for (ProducerElement element : elements) {
            minBatchId = Math.min(minBatchId, element.getMinBatchId());
            maxBatchId = Math.max(maxBatchId, element.getMaxBatchId());
        }

        ByteArrayOutputStream combinedOutput = new ByteArrayOutputStream();
        Schema actualSchema = null;
        VectorSchemaRoot root = null;
        ArrowStreamWriter writer = null;

        try {
            // Process all elements in a single pass
            for (int i = 0; i < elements.size(); i++) {
                ProducerElement element = elements.get(i);
                try (InputStream in = element.read();
                     ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {

                    if (i == 0) {
                        // First element - get schema and initialize writer
                        actualSchema = reader.getVectorSchemaRoot().getSchema();
                        root = VectorSchemaRoot.create(actualSchema, allocator);
                        writer = createArrowStreamWriter(root, combinedOutput, compressionType);
                        writer.start();
                    }

                    // Process all batches from this element
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot sourceBatch = reader.getVectorSchemaRoot();

                        // Use VectorUnloader/VectorLoader pattern to copy the batch
                        try (ArrowRecordBatch recordBatch =
                                     new VectorUnloader(sourceBatch).getRecordBatch()) {

                            new VectorLoader(root).load(recordBatch);
                            writer.writeBatch();
                        }
                    }
                }
            }

            if (writer != null) {
                writer.end();
            }
        } finally {
            // Ensure resources are closed to prevent memory leaks
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    AbstractArrowProducer.logger.warn("Failed to close ArrowStreamWriter", e);
                }
            }
            if (root != null) {
                root.close();
            }
        }

        return new MemoryElement(combinedOutput.toByteArray(), minBatchId, maxBatchId);
    }

    /**
     * Creates an ArrowStreamWriter with optional compression.
     *
     * @param root The VectorSchemaRoot to write
     * @param outputStream The output stream to write to
     * @param compressionType The compression type to use
     * @return A configured ArrowStreamWriter
     */
    static ArrowStreamWriter createArrowStreamWriter(
            VectorSchemaRoot root,
            OutputStream outputStream,
            CompressionUtil.CodecType compressionType) {
        if (compressionType == CompressionUtil.CodecType.NO_COMPRESSION) {
            return new ArrowStreamWriter(root, null, outputStream);
        }
        return new ArrowStreamWriter(
                root,
                null,
                Channels.newChannel(outputStream),
                IpcOption.DEFAULT,
                CommonsCompressionFactory.INSTANCE,
                compressionType);
    }

    void addRow(JavaRow row);
    long getMaxInMemorySize();

    long getMaxOnDiskSize();

    abstract class AbstractArrowProducer implements ArrowProducer {

        private static final Logger logger  = LoggerFactory.getLogger(AbstractArrowProducer.class);
        private final BlockingQueue<ProducerElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        protected final Clock clock;
        private volatile boolean shutdown = false;
        private volatile boolean forceShutdown = false;

        protected final Thread senderThread;

        private long inMemorySize = 0;
        private long onDiskSize = 0;

        // Counters for tracking send statistics
        private final java.util.concurrent.atomic.AtomicLong totalRetryCount = new java.util.concurrent.atomic.AtomicLong(0);
        private final java.util.concurrent.atomic.AtomicLong droppedElementCount = new java.util.concurrent.atomic.AtomicLong(0);
        private final java.util.concurrent.atomic.AtomicLong sentElementCount = new java.util.concurrent.atomic.AtomicLong(0);

        private final long minBatchSize;

        private final long maxBatchSize;

        private final Duration maxDataSendInterval;

        private final int retryCount;

        private final long retryIntervalMillis;

        private final java.util.List<String> projections;

        private final java.util.List<String> partitionBy;

        private final CompressionUtil.CodecType compressionType;

        private Instant lastSent;
        private Bucket currentBucket;
        final Schema schema;

        protected final RootAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);

        private final ScheduledExecutorService executorService;

        private long currentBatchId = 0;


        public AbstractArrowProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> projections, java.util.List<String> partitionBy){
            this(minBatchSize, maxBatchSize, maxDataSendInterval, schema, clock, retryCount, retryIntervalMillis, projections, partitionBy, CompressionUtil.CodecType.ZSTD, Executors.newSingleThreadScheduledExecutor());
        }

        public AbstractArrowProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> projections, java.util.List<String> partitionBy, CompressionUtil.CodecType compressionType){
            this(minBatchSize, maxBatchSize, maxDataSendInterval, schema, clock, retryCount, retryIntervalMillis, projections, partitionBy, compressionType, Executors.newSingleThreadScheduledExecutor());
        }

        public AbstractArrowProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> projections, java.util.List<String> partitionBy, ScheduledExecutorService scheduledExecutorService){
            this(minBatchSize, maxBatchSize, maxDataSendInterval, schema, clock, retryCount, retryIntervalMillis, projections, partitionBy, CompressionUtil.CodecType.ZSTD, scheduledExecutorService);
        }

        public AbstractArrowProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> projections, java.util.List<String> partitionBy, CompressionUtil.CodecType compressionType, ScheduledExecutorService scheduledExecutorService ){
            // Validate parameters
            if (minBatchSize <= 0) {
                throw new IllegalArgumentException("minBatchSize must be positive, got: " + minBatchSize);
            }
            if (maxBatchSize <= 0) {
                throw new IllegalArgumentException("maxBatchSize must be positive, got: " + maxBatchSize);
            }
            if (maxBatchSize < minBatchSize) {
                throw new IllegalArgumentException("maxBatchSize must be >= minBatchSize, got: " + maxBatchSize + " < " + minBatchSize);
            }
            if (maxDataSendInterval == null || maxDataSendInterval.isNegative() || maxDataSendInterval.isZero()) {
                throw new IllegalArgumentException("maxDataSendInterval must be positive, got: " + maxDataSendInterval);
            }
            if (schema == null) {
                throw new IllegalArgumentException("schema must not be null");
            }
            if (clock == null) {
                throw new IllegalArgumentException("clock must not be null");
            }
            if (retryCount < 0) {
                throw new IllegalArgumentException("retryCount must be non-negative, got: " + retryCount);
            }
            if (retryIntervalMillis < 0) {
                throw new IllegalArgumentException("retryIntervalMillis must be non-negative, got: " + retryIntervalMillis);
            }
            if (projections == null) {
                throw new IllegalArgumentException("projections must not be null");
            }
            if (partitionBy == null) {
                throw new IllegalArgumentException("partitionBy must not be null");
            }
            if (compressionType == null) {
                throw new IllegalArgumentException("compressionType must not be null");
            }
            if (scheduledExecutorService == null) {
                throw new IllegalArgumentException("scheduledExecutorService must not be null");
            }

            logger.info("FlightSender started at {} with send interval {}, retryCount {}, retryIntervalMillis {}, projections {}, partitionBy {}, compression {}", clock.instant(), maxDataSendInterval, retryCount, retryIntervalMillis, projections, partitionBy, compressionType);
            this.minBatchSize = minBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.maxDataSendInterval = maxDataSendInterval;
            this.retryCount = retryCount;
            this.retryIntervalMillis = retryIntervalMillis;
            this.projections = List.copyOf(projections);
            this.partitionBy = List.copyOf(partitionBy);
            this.compressionType = compressionType;
            this.clock = clock;
            this.schema = schema;
            this.lastSent = clock.instant();
            this.currentBucket = new Bucket();
            this.senderThread = new Thread(() -> {
                boolean error = false;
                while (!shutdown || !queue.isEmpty()) {
                    try {
                        var current = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (current != null) {
                            java.util.List<ProducerElement> batch = new java.util.ArrayList<>();
                            batch.add(current);
                            long batchSize = current.length();

                            // Try to batch additional elements from the queue
                            // Stop batching if we reach 100 elements or maxBatchSize bytes
                            ProducerElement additional;
                            while (batch.size() < 100 && (additional = queue.peek()) != null) {
                                long additionalSize = additional.length();
                                if (batchSize + additionalSize > maxBatchSize) {
                                    // Don't remove the element, just stop batching
                                    break;
                                }
                                // Size is OK, now actually remove it from the queue
                                queue.poll();
                                batch.add(additional);
                                batchSize += additionalSize;
                            }

                            try {
                                // Send the batch (combining will happen inside doSendWithRetry if needed)
                                doSendWithRetry(batch);

                                // Update state for all original elements
                                for (ProducerElement element : batch) {
                                    updateState(element);
                                }
                            } catch (Exception e) {
                                error = true;
                                shutdown = true;
                                forceShutdown = true;
                                logger.atError().setCause(e).log("Error sending data");
                                logger.atError().setCause(e).log("Shutting down because of exception");
                                throw e;
                            } finally {
                                // Always close all original elements after sending (successful or not)
                                for (ProducerElement element : batch) {
                                    element.close();
                                }
                            }
                        }
                    } catch (InterruptedException | IOException e) {
                        if (shutdown) {
                            // Restore interrupt status before draining
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }

                            // Drain and process remaining items
                            java.util.List<ProducerElement> batch = new java.util.ArrayList<>();
                            ProducerElement element;
                            while ((element = queue.poll()) != null) {
                                batch.add(element);
                            }
                            if (!batch.isEmpty() && !error) {
                                try {
                                    // Send the batch (combining will happen inside doSendWithRetry if needed)
                                    doSendWithRetry(batch);

                                    // Update state for all original elements
                                    for (ProducerElement el : batch) {
                                        updateState(el);
                                    }
                                } catch (InterruptedException ex) {
                                    // If interrupted again, restore interrupt status and close all elements
                                    Thread.currentThread().interrupt();
                                } catch (IOException ex) {
                                    logger.atError().setCause(ex).log("Error sending message while closing");
                                } finally {
                                    // Close all original elements
                                    for (ProducerElement el : batch) {
                                        el.close();
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            });
            this.executorService = scheduledExecutorService;
            // Set daemon flag before thread is started
            this.senderThread.setDaemon(true);
            this.senderThread.start();
            executorService.submit(() -> enqueueOrScheduleCurrentBucket(maxDataSendInterval));
        }

        private synchronized void enqueueCurrentBucket(){
            if (this.currentBucket.size() > 0) {
                try (var c = bufferAllocator.newChildAllocator("child", minBatchSize, Long.MAX_VALUE)) {
                    var bytes = this.currentBucket.getArrowBytes(schema, c, compressionType);
                    if (bytes != null && bytes.length > 0) {
                        enqueue(bytes);
                        lastSent = clock.instant();
                        currentBucket = new Bucket();
                    }
                }
            }
        }


        private synchronized void enqueueOrScheduleCurrentBucket(Duration maxDataSendInterval){

            var now = clock.instant();
            logger.debug("Checking bucket send at {}", now);
            var toBeSent = lastSent.plus(maxDataSendInterval);
            var timeRemaining = maxDataSendInterval;
            try {
                if (toBeSent.isBefore(now) || toBeSent.equals(now)) {
                    enqueueCurrentBucket();
                } else {
                    timeRemaining = Duration.between(now, toBeSent);
                }
            } finally {
                logger.debug("Next bucket send scheduled in {}", timeRemaining);
                if (!shutdown) {
                    executorService.schedule(() -> enqueueOrScheduleCurrentBucket(maxDataSendInterval),
                            timeRemaining.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }
        @Override
        public synchronized void addRow(JavaRow row) {
            if (shutdown) {
                throw new IllegalStateException("Sender is shutdown, cannot enqueue");
            }
            var currentSize = currentBucket.add(row);
            if (currentSize > minBatchSize) {
                try(var c = bufferAllocator.newChildAllocator("child",  minBatchSize, Long.MAX_VALUE)) {
                    var arrowBytes = currentBucket.getArrowBytes(schema, c, compressionType);
                    enqueue(arrowBytes);
                    this.lastSent = clock.instant();
                    currentBucket = new Bucket();
                }
            }
        }

        @Override
        public synchronized void enqueue(byte[] input) {
            if (shutdown) {
                throw new IllegalStateException("Sender is shutdown, cannot enqueue");
            }

            StoreStatus storeStatus = getStoreStatus(input.length);
            switch (storeStatus) {
                case FULL:
                    throw new IllegalStateException("queue is full");
                case IN_MEMORY:
                    queue.add(new MemoryElement(input, currentBatchId++));
                    break;
                case ON_DISK:
                    queue.add(new FileMappedMemoryElement(input, currentBatchId++));
                    break;
            }
        }

        public synchronized StoreStatus getStoreStatus(int size) {
            if (inMemorySize + size < getMaxInMemorySize()) {
                inMemorySize += size;
                return StoreStatus.IN_MEMORY;
            }

            if (onDiskSize + size < getMaxOnDiskSize()) {
                onDiskSize += size;
                return StoreStatus.ON_DISK;
            }
            return StoreStatus.FULL;
        }

        private synchronized void updateState(ProducerElement producerElement) {
            if (producerElement instanceof MemoryElement) {
                inMemorySize -= producerElement.length();
            } else {
                onDiskSize -= producerElement.length();
            }
        }

        private void doSendWithRetry(List<ProducerElement> elements) throws InterruptedException, IOException {
            ProducerElement elementToSend = null;
            boolean shouldCloseCombinedElement = false;

            try {
                // Determine what element to send
                if (elements.size() == 1) {
                    // Single element - send it directly without combining
                    elementToSend = elements.get(0);
                } else {
                    // Multiple elements or compression needed - combine/compress them first
                    try (org.apache.arrow.memory.BufferAllocator childAllocator =
                            bufferAllocator.newChildAllocator("combine-batch", 0, Long.MAX_VALUE)) {
                        elementToSend = createCombinedReader(elements, schema, childAllocator, compressionType);
                        shouldCloseCombinedElement = true;
                    }
                }

                // Retry logic for sending
                int attempt = 0;
                Exception lastException = null;

                while (attempt <= retryCount) {
                    try {
                        doSend(elementToSend);
                        sentElementCount.incrementAndGet();
                        if (attempt > 0) {
                            logger.info("Successfully sent element after {} retries", attempt);
                        }
                        return; // Success, exit the retry loop
                    } catch (InterruptedException e) {
                        // Don't retry on interruption, propagate immediately
                        droppedElementCount.incrementAndGet();
                        throw e;
                    } catch (Exception e) {
                        lastException = e;
                        attempt++;
                        totalRetryCount.incrementAndGet();

                        // Don't retry if we're force shutting down (only skip retries on forced shutdown, not graceful)
                        if (forceShutdown) {
                            logger.warn("Sender is force shutting down, skipping retry for failed send: {}", e.getMessage());
                            droppedElementCount.incrementAndGet();
                            return;
                        }

                        if (attempt <= retryCount) {
                            logger.warn("Failed to send element (attempt {}/{}): {}", attempt, retryCount + 1, e.getMessage());
                            try {
                                Thread.sleep(retryIntervalMillis);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                droppedElementCount.incrementAndGet();
                                throw ie;
                            }
                        } else {
                            logger.error("Failed to send element after {} attempts", attempt, e);
                            droppedElementCount.incrementAndGet();
                        }
                    }
                }

                // If we exhausted all retries, log the final failure
                if (lastException != null) {
                    logger.error("Exhausted all {} retry attempts, element will be dropped", retryCount + 1, lastException);
                }
            } finally {
                // Only close the combined element if we created one
                if (shouldCloseCombinedElement && elementToSend != null) {
                    elementToSend.close();
                }
            }
        }

        abstract protected void doSend(ProducerElement element) throws InterruptedException;

        protected int getRetryCount() {
            return retryCount;
        }

        protected long getRetryIntervalMillis() {
            return retryIntervalMillis;
        }

        protected java.util.List<String> getProjections() {
            return projections;
        }

        protected java.util.List<String> getPartitionBy() {
            return partitionBy;
        }

        protected CompressionUtil.CodecType getCompressionType() {
            return compressionType;
        }

        protected Schema getSchema() {
            return schema;
        }

        protected long getMaxBatchSize() {
            return maxBatchSize;
        }

        /**
         * Returns the total number of retry attempts made during the lifetime of this producer.
         */
        protected long getTotalRetryCount() {
            return totalRetryCount.get();
        }

        /**
         * Returns the number of elements that were dropped (failed to send after all retries).
         */
        protected long getDroppedElementCount() {
            return droppedElementCount.get();
        }

        /**
         * Returns the number of elements successfully sent.
         */
        protected long getSentElementCount() {
            return sentElementCount.get();
        }

        @Override
        public void close()  {
            // Send final bucket before shutdown
            enqueueCurrentBucket();

            // Set shutdown flag in minimal synchronized block
            synchronized (this) {
                shutdown = true;
            }

            // Shutdown the scheduled executor service (outside synchronized block to avoid deadlock)
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    var toExecute = executorService.shutdownNow();
                    logger.warn("ExecutorService did not terminate gracefully, forced shutdown of {} tasks", toExecute.size());
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
                logger.atError().setCause(e).log("error closing executor service");
            }

            // Wait for the sender thread to finish processing the queue naturally
            // The sender thread will exit when shutdown=true and queue is empty
            try {
                // Give it reasonable time to finish processing (longer for graceful shutdown with retries)
                senderThread.join(3000);

                // If it's still running, force shutdown and interrupt it
                if (senderThread.isAlive()) {
                    logger.warn("Sender thread did not finish gracefully, forcing shutdown");
                    forceShutdown = true;
                    senderThread.interrupt();
                    senderThread.join(5000);
                }
            } catch (InterruptedException e) {
                logger.atError().setCause(e).log("error closing sender");
                forceShutdown = true;
                senderThread.interrupt();
                Thread.currentThread().interrupt();
            }

            // Clean up remaining queue items
            cleanupQueue();

            // Log statistics
            logCloseStatistics();

            // Close allocator AFTER sender thread has stopped to avoid race condition
            bufferAllocator.close();
        }

        /**
         * Logs statistics when the producer is closed. Can be overridden by subclasses
         * to add additional context.
         */
        protected void logCloseStatistics() {
            long sent = sentElementCount.get();
            long dropped = droppedElementCount.get();
            long retries = totalRetryCount.get();

            if (dropped > 0) {
                logger.error("Producer closed with {} unsent/dropped elements. Stats: sent={}, retries={}",
                        dropped, sent, retries);
            } else {
                logger.info("Producer closed. Stats: sent={}, dropped={}, retries={}",
                        sent, dropped, retries);
            }
        }

        private void cleanupQueue() {
            ProducerElement element;
            int droppedInCleanup = 0;
            while ((element = queue.poll()) != null) {
                synchronized(this) {
                    if (element instanceof MemoryElement) {
                        inMemorySize -= element.length();
                    } else {
                        onDiskSize -= element.length();
                    }
                }
                droppedInCleanup++;
                droppedElementCount.incrementAndGet();
                // Close the element to cleanup resources
                element.close();
            }
            if (droppedInCleanup > 0) {
                logger.error("Dropped {} unsent elements during cleanup", droppedInCleanup);
            }
        }
    }

    interface ProducerElement extends Closeable {
        InputStream read();
        long length();
        long getMinBatchId();
        long getMaxBatchId();

        @Override
        void close();
    }

    class MemoryElement implements ProducerElement {
        final byte[] data; // package-private for testing
        private final long minBatchId;
        private final long maxBatchId;

        public MemoryElement(byte[] data, long batchId) {
            this(data, batchId, batchId);
        }

        public MemoryElement(byte[] data, long minBatchId, long maxBatchId) {
            this.data = data;
            this.minBatchId = minBatchId;
            this.maxBatchId = maxBatchId;
        }

        @Override
        public InputStream read() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public long getMinBatchId() {
            return minBatchId;
        }

        @Override
        public long getMaxBatchId() {
            return maxBatchId;
        }

        @Override
        public void close() {
            // No-op: in-memory elements don't require cleanup
        }
    }

    class FileMappedMemoryElement implements ProducerElement {
        private static final Logger logger = LoggerFactory.getLogger(FileMappedMemoryElement.class);
        private final Path tempFile;
        private final long length;
        private final long minBatchId;
        private final long maxBatchId;

        public FileMappedMemoryElement(byte[] data, long batchId) {
            this(data, batchId, batchId);
        }

        public FileMappedMemoryElement(byte[] data, long minBatchId, long maxBatchId) {
            this.minBatchId = minBatchId;
            this.maxBatchId = maxBatchId;
            Path temp = null;
            try {
                temp = Files.createTempFile("flight-", ".arrow");
                Files.write(temp, data);
                this.tempFile = temp;
                this.length = data.length;
            } catch (IOException e) {
                // Clean up temp file if write failed
                if (temp != null) {
                    try {
                        Files.deleteIfExists(temp);
                    } catch (IOException cleanupException) {
                        logger.warn("Failed to cleanup temp file after error: {}", temp, cleanupException);
                    }
                }
                throw new RuntimeException("Failed to create file-mapped element", e);
            }
        }

        @Override
        public void close() {
            try {
                if (tempFile != null && Files.exists(tempFile)) {
                    Files.delete(tempFile);
                }
            } catch (IOException e) {
                logger.warn("Failed to delete temporary file: {}", tempFile, e);
            }
        }

        @Override
        public InputStream read() {
            try {
                return Files.newInputStream(tempFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public long getMinBatchId() {
            return minBatchId;
        }

        @Override
        public long getMaxBatchId() {
            return maxBatchId;
        }
    }

    final class Bucket {

        private final List<JavaRow> buffer = new ArrayList<>();
        private long size = 0;
        private boolean serialized = false;

        public Bucket() {

        }

        public long add(JavaRow row) {
            if(serialized) {
                throw new IllegalStateException("the bucket is already serialized");
            }
            buffer.add(row);
            size += row.getActualSize();
            return size;
        }

        public long size() {
            return size;
        }

        public byte[] getArrowBytes(Schema schema, BufferAllocator allocator) {
            return getArrowBytes(schema, allocator, CompressionUtil.CodecType.ZSTD);
        }

        public byte[] getArrowBytes(Schema schema, BufferAllocator allocator, CompressionUtil.CodecType compressionType) {
            if (serialized || buffer.isEmpty()) {
                return null;
            }
            serialized = true;

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                 ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = createArrowStreamWriter(root, out, compressionType)) {
                JavaRow[] rows = buffer.toArray(JavaRow[]::new);
                VectorSchemaRootWriter.of(schema).writeToVector(rows, root);
                root.setRowCount(rows.length);
                writer.start();
                writer.writeBatch();
                writer.end();
                return out.toByteArray();

            } catch (Exception e) {
                throw new RuntimeException("Arrow serialization failed", e);
            }
        }
    }
}
