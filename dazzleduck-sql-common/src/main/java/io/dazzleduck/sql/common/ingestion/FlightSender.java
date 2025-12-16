package io.dazzleduck.sql.common.ingestion;

import io.dazzleduck.sql.common.types.JavaRow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;

public interface FlightSender {

    void close() throws InterruptedException;

    enum StoreStatus {
        IN_MEMORY, ON_DISK, FULL
    }

    void enqueue(byte[] input);

    long getMaxInMemorySize();

    long getMaxOnDiskSize();

    abstract class AbstractFlightSender implements FlightSender {
        private final BlockingQueue<SendElement> queue = new ArrayBlockingQueue<>(1024 * 1024);
        private final Clock clock;
        private volatile boolean shutdown = false;
        private volatile boolean started = false;

        protected final Thread senderThread;

        private long inMemorySize = 0;
        private long onDiskSize = 0;

        private long maxBatchSize;

        private Duration maxDataSendInterval;

        private Instant lastSent;

        private Bucket currentBucket =  new Bucket();

        private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        public AbstractFlightSender(long maxBatchSize, Duration maxDataSendInterval, Clock clock){
            this.maxBatchSize = maxBatchSize;
            this.maxDataSendInterval = maxDataSendInterval;
            this.clock = clock;
            this.lastSent = clock.instant();



            this.senderThread = new Thread(() -> {
                while (!shutdown || !queue.isEmpty()) {
                    try {
                        var current = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (current != null) {
                            doSend(current);
                            updateState(current);
                        }
                    } catch (InterruptedException e) {
                        if (shutdown) {
                            // Drain and process remaining items
                            SendElement element;
                            while ((element = queue.poll()) != null) {
                                try {
                                    doSend(element);
                                    updateState(element);
                                } catch (InterruptedException ex) {
                                    // If interrupted again, exit
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }
            });

            // Set daemon flag before thread is started
            this.senderThread.setDaemon(true);
            this.senderThread.start();

            executorService.schedule(this::sendOrScheduleCurrentBucket, maxDataSendInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void sendCurrentBucket(){
            var bytes = this.currentBucket.getArrowBytes();
            if (bytes != null && bytes.length > 0) {
                enqueue(bytes);
                lastSent = clock.instant();
            }
        }

        private synchronized void sendOrScheduleCurrentBucket(){
            var now = clock.instant();
            var toBeSent = lastSent.plus(maxDataSendInterval);
            if(toBeSent.isBefore(now) || toBeSent.equals(now)){
                sendCurrentBucket();
            } else {
                var timeRemaining = Duration.between(now, toBeSent);
                executorService.schedule(this::sendOrScheduleCurrentBucket, timeRemaining.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
        public synchronized void addRow(JavaRow row) {
            if (shutdown) {
                throw new IllegalStateException("Sender is shutdown, cannot enqueue");
            }
            var currentSize = currentBucket.add(row);
            if (currentSize > maxBatchSize) {
                var arrowBytes = currentBucket.getArrowBytes();
                this.lastSent = Clock.systemDefaultZone().instant();
                enqueue(arrowBytes);
                currentBucket = new Bucket();
            }
        }

        @Override
        public void enqueue(byte[] input) {
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
                ((FileMappedMemoryElement) sendElement).cleanup();
            }
        }

        abstract protected void doSend(SendElement element) throws InterruptedException;

        public synchronized void start() {
            if (started) {
                throw new IllegalStateException("FlightSender has already been started and cannot be restarted");
            }
            if (shutdown) {
                throw new IllegalStateException("FlightSender has been shutdown and cannot be started");
            }

            started = true;
            senderThread.start();
        }

        @Override
        public synchronized void close() throws InterruptedException {
            sendCurrentBucket();
            shutdown = true;

            // Shutdown the scheduled executor service
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Shutdown the sender thread
            senderThread.interrupt();
            senderThread.join();
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

                if (element instanceof FileMappedMemoryElement) {
                    ((FileMappedMemoryElement) element).cleanup();
                }
            }
        }
    }

    public interface SendElement {
        InputStream read();
        long length();
    }

    public class MemoryElement implements SendElement {
        private byte[] data;

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
    }

    public class FileMappedMemoryElement implements SendElement {
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

        public void cleanup() {
            try {
                if (tempFile != null && Files.exists(tempFile)) {
                    Files.delete(tempFile);
                }
            } catch (IOException e) {
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
}
