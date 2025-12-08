package io.dazzleduck.sql.common.ingestion;

import java.util.concurrent.CountDownLatch;

public class FlightSenderTest {
    // Use  on demand Sender to test it
}

class OnDemandSender extends FlightSender.AbstractFlightSender {

    private final long maxOnDiskSize;
    private final long maxInMemorySize;
    private final CountDownLatch latch;

    public OnDemandSender(long maxInMemorySize, long maxOnDiskSize, CountDownLatch latch) {
        this.maxInMemorySize = maxInMemorySize;
        this.maxOnDiskSize = maxOnDiskSize;
        this.latch = latch;
    }

    @Override
    public long getMaxInMemorySize() {
        return maxInMemorySize;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxOnDiskSize;
    }

    @Override
    protected synchronized void doSend(SendElement element) throws InterruptedException {
        latch.await();
    }
}
