package io.dazzleduck.sql.client;

import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.common.types.VectorSchemaRootWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;  // FIXED: Use Arrow Schema
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public interface FlightProducer extends Closeable {

    void close();

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void enqueue(byte[] input);
    void addRow(JavaRow row);
    long getMaxInMemorySize();

    long getMaxOnDiskSize();

    abstract class AbstractFlightProducer implements FlightProducer {

        private static final Logger logger  = LoggerFactory.getLogger(AbstractFlightProducer.class);
        private final BlockingQueue<SendElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        protected final Clock clock;
        private volatile boolean shutdown = false;

        protected final Thread senderThread;

        private long inMemorySize = 0;
        private long onDiskSize = 0;

        private final long minBatchSize;

        private final long maxBatchSize;

        private final Duration maxDataSendInterval;

        private final int retryCount;

        private final long retryIntervalMillis;

        private final java.util.List<String> transformations;

        private final java.util.List<String> partitionBy;

        private Instant lastSent;
        private Bucket currentBucket;
        final Schema schema;

        protected final RootAllocator bufferAllocator = new RootAllocator(Long.MAX_VALUE);

        private final ScheduledExecutorService executorService;


        public AbstractFlightProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> transformations, java.util.List<String> partitionBy){
            this(minBatchSize, maxBatchSize, maxDataSendInterval, schema, clock, retryCount, retryIntervalMillis, transformations, partitionBy, Executors.newSingleThreadScheduledExecutor());
        }
        public AbstractFlightProducer(long minBatchSize, long maxBatchSize, Duration maxDataSendInterval, Schema schema, Clock clock, int retryCount, long retryIntervalMillis, java.util.List<String> transformations, java.util.List<String> partitionBy, ScheduledExecutorService scheduledExecutorService ){
            logger.info("FlightSender started at {} with send interval {}, retryCount {}, retryIntervalMillis {}, transformations {}, partitionBy {}", clock.instant(), maxDataSendInterval, retryCount, retryIntervalMillis, transformations, partitionBy);
            this.minBatchSize = minBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.maxDataSendInterval = maxDataSendInterval;
            this.retryCount = retryCount;
            this.retryIntervalMillis = retryIntervalMillis;
            this.transformations = List.copyOf(transformations);
            this.partitionBy = List.copyOf(partitionBy);
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
                            java.util.List<SendElement> batch = new java.util.ArrayList<>();
                            batch.add(current);
                            long batchSize = current.length();

                            // Try to batch additional elements from the queue
                            // Stop batching if we reach 100 elements or maxBatchSize bytes
                            SendElement additional;
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
                                doSendWithRetry(batch);
                                for (SendElement element : batch) {
                                    updateState(element);
                                }
                            } catch (Exception e) {
                                error = true;
                                shutdown = true;
                                logger.atError().setCause(e).log("Error sending data");
                                logger.atError().setCause(e).log("Shutting down because of exception");
                                throw e;
                            } finally {
                                // Always close all elements after sending (successful or not)
                                for (SendElement element : batch) {
                                    element.close();
                                }
                            }
                        }
                    } catch (InterruptedException | IOException e) {
                        if (shutdown) {
                            // Drain and process remaining items
                            java.util.List<SendElement> batch = new java.util.ArrayList<>();
                            SendElement element;
                            while ((element = queue.poll()) != null) {
                                batch.add(element);
                            }
                            if (!batch.isEmpty() && !error) {
                                try {
                                    doSendWithRetry(batch);
                                    for (SendElement el : batch) {
                                        updateState(el);
                                    }
                                } catch (InterruptedException ex) {
                                    // If interrupted again, just close all elements
                                } catch (IOException ex) {
                                    logger.atError().setCause(ex).log("Error sending message while closing");
                                } finally {
                                    for (SendElement el : batch) {
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
            executorService.submit(() -> sendOrScheduleCurrentBucket(maxDataSendInterval));
        }

        /**
         * Creates a combined ArrowStreamReader from a list of SendElements.
         * This utility method reads all Arrow batches from the input elements and combines them
         * into a single Arrow stream.
         *
         * @param elements List of SendElements to combine
         * @param schema The Arrow schema for the data
         * @param allocator Buffer allocator for Arrow operations
         * @return ArrowStreamReader containing all batches from the input elements
         * @throws IOException if reading or writing Arrow data fails
         */
        protected static ArrowStreamReader createCombinedReader(
                java.util.List<SendElement> elements,
                Schema schema,
                BufferAllocator allocator) throws IOException {

            if (elements.isEmpty()) {
                // For empty list, use the provided schema
                ByteArrayOutputStream emptyOutput = new ByteArrayOutputStream();
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                     ArrowStreamWriter writer = new ArrowStreamWriter(root, null, emptyOutput)) {
                    writer.start();
                    writer.end();
                }
                return new ArrowStreamReader(new ByteArrayInputStream(emptyOutput.toByteArray()), allocator);
            }

            ByteArrayOutputStream combinedOutput = new ByteArrayOutputStream();
            Schema actualSchema = null;
            VectorSchemaRoot root = null;
            ArrowStreamWriter writer = null;

            try {
                // Process all elements in a single pass
                for (int i = 0; i < elements.size(); i++) {
                    SendElement element = elements.get(i);
                    try (InputStream in = element.read();
                         ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {

                        if (i == 0) {
                            // First element - get schema and initialize writer
                            actualSchema = reader.getVectorSchemaRoot().getSchema();
                            root = VectorSchemaRoot.create(actualSchema, allocator);
                            writer = new ArrowStreamWriter(root, null, combinedOutput);
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
                // Ensure root is closed to prevent memory leaks
                if (root != null) {
                    root.close();
                }
            }

            byte[] combinedBytes = combinedOutput.toByteArray();
            return new ArrowStreamReader(new ByteArrayInputStream(combinedBytes), allocator);
        }

        private synchronized void sendCurrentBucket(){
            if (this.currentBucket.size() > 0) {
                try (var c = bufferAllocator.newChildAllocator("child", minBatchSize, Long.MAX_VALUE)) {
                    var bytes = this.currentBucket.getArrowBytes(schema, c);
                    if (bytes != null && bytes.length > 0) {
                        enqueue(bytes);
                        lastSent = clock.instant();
                        currentBucket = new Bucket();
                    }
                }
            }
        }


        private synchronized void sendOrScheduleCurrentBucket(Duration maxDataSendInterval){

            var now = clock.instant();
            logger.debug("Checking bucket send at {}", now);
            var toBeSent = lastSent.plus(maxDataSendInterval);
            var timeRemaining = maxDataSendInterval;
            try {
                if (toBeSent.isBefore(now) || toBeSent.equals(now)) {
                    sendCurrentBucket();
                } else {
                    timeRemaining = Duration.between(now, toBeSent);
                }
            } finally {
                logger.debug("Next bucket send scheduled in {}", timeRemaining);
                if (!shutdown) {
                    executorService.schedule(() -> sendOrScheduleCurrentBucket(maxDataSendInterval),
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
                    var arrowBytes = currentBucket.getArrowBytes(schema, c);
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

            var storeStatus = getStoreStatus(input.length);
            switch (storeStatus) {
                case FULL -> throw new IllegalStateException("queue is full");
                case IN_MEMORY -> queue.add(new MemoryElement(input));
                case ON_DISK -> queue.add(new FileMappedMemoryElement(input));
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

        private synchronized void updateState(SendElement sendElement) {
            if (sendElement instanceof MemoryElement) {
                inMemorySize -= sendElement.length();
            } else {
                onDiskSize -= sendElement.length();
            }
        }

        private void doSendWithRetry(java.util.List<SendElement> elements) throws InterruptedException, IOException {
            int attempt = 0;
            Exception lastException = null;

            while (attempt <= retryCount) {
                try {
                    doSend(elements);
                    if (attempt > 0) {
                        logger.info("Successfully sent {} elements after {} retries", elements.size(), attempt);
                    }
                    return; // Success, exit the retry loop
                } catch (InterruptedException e) {
                    // Don't retry on interruption, propagate immediately
                    throw e;
                } catch (Exception e) {
                    lastException = e;
                    attempt++;

                    // Don't retry if we're shutting down
                    if (shutdown) {
                        logger.warn("Sender is shutting down, skipping retry for failed send: {}", e.getMessage());
                        return;
                    }

                    if (attempt <= retryCount) {
                        logger.warn("Failed to send {} elements (attempt {}/{}): {}", elements.size(), attempt, retryCount + 1, e.getMessage());
                        try {
                            Thread.sleep(retryIntervalMillis);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw ie;
                        }
                    } else {
                        logger.error("Failed to send {} elements after {} attempts", elements.size(), attempt, e);
                    }
                }
            }

            // If we exhausted all retries, log the final failure
            if (lastException != null) {
                logger.error("Exhausted all {} retry attempts, {} elements will be dropped", retryCount + 1, elements.size(), lastException);
            }
        }

        abstract protected void doSend(java.util.List<SendElement> elements) throws InterruptedException;

        protected int getRetryCount() {
            return retryCount;
        }

        protected long getRetryIntervalMillis() {
            return retryIntervalMillis;
        }

        protected java.util.List<String> getTransformations() {
            return transformations;
        }

        protected java.util.List<String> getPartitionBy() {
            return partitionBy;
        }

        protected Schema getSchema() {
            return schema;
        }

        protected long getMaxBatchSize() {
            return maxBatchSize;
        }

        @Override
        public void close()  {
            synchronized (this) {
                sendCurrentBucket();
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

            // Shutdown the sender thread
            senderThread.interrupt();
            bufferAllocator.close();
            try {
                senderThread.join();
            } catch (InterruptedException e) {
                logger.atError().setCause(e).log("error closing sender");
            }
            cleanupQueue();
        }

        private void cleanupQueue() {
            SendElement element;
            while ((element = queue.poll()) != null) {
                synchronized(this) {
                    if (element instanceof MemoryElement) {
                        inMemorySize -= element.length();
                    } else {
                        onDiskSize -= element.length();
                    }
                }
                // Close the element to cleanup resources
                element.close();
            }
        }
    }

    interface SendElement extends Closeable {
        InputStream read();
        long length();

        @Override
        void close();
    }

    class MemoryElement implements SendElement {
        final byte[] data; // package-private for testing

        public MemoryElement(byte[] data) {
            this.data = data;
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
        public void close() {
            // No-op: in-memory elements don't require cleanup
        }
    }

    class FileMappedMemoryElement implements SendElement {
        private static final Logger logger = LoggerFactory.getLogger(FileMappedMemoryElement.class);
        private final Path tempFile;
        private final long length;

        public FileMappedMemoryElement(byte[] data) {
            try {
                tempFile = Files.createTempFile("flight-", ".arrow");
                Files.write(tempFile, data);
                this.length = data.length;
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            if (serialized || buffer.isEmpty()) {
                return null;
            }
            serialized = true;

            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                 ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
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
