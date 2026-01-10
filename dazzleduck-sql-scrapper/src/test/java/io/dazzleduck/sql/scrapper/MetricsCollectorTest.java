package io.dazzleduck.sql.scrapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricsCollector - main collector with tailing support.
 *
 * Note: HttpProducer handles actual HTTP sending asynchronously with authentication,
 * so these tests focus on the collector's scraping and buffering behavior.
 */
@Tag("slow")
class MetricsCollectorTest {

    private MockWebServer targetServer;
    private CollectorProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        targetServer = new MockWebServer();
        targetServer.start();

        properties = new CollectorProperties();
        properties.setEnabled(true);
        properties.setTargets(List.of(targetServer.url("/actuator/prometheus").toString()));
        properties.setServerUrl("http://localhost:8081");  // HttpProducer will handle auth
        properties.setPath("metrics");
        properties.setUsername("test");
        properties.setPassword("test");
        properties.setScrapeIntervalMs(20);
        properties.setFlushThreshold(5);
        properties.setFlushIntervalMs(100);
        properties.setConnectionTimeoutMs(500);
        properties.setReadTimeoutMs(500);
        properties.setMaxRetries(0);
        properties.setMinBatchSize(1024);
        properties.setMaxBatchSize(16 * 1024 * 1024);
        properties.setMaxInMemorySize(10 * 1024 * 1024);
        properties.setMaxOnDiskSize(1024 * 1024 * 1024);

        // Reset sequence for consistent tests
        CollectedMetric.resetSequence();
    }

    @AfterEach
    void tearDown() throws Exception {
        targetServer.shutdown();
    }

    @Test
    @DisplayName("Should start and stop collector")
    void startAndStop() {
        MetricsCollector collector = new MetricsCollector(properties);

        assertFalse(collector.isRunning());

        collector.start();
        assertTrue(collector.isRunning());

        collector.stop();
        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should not start if disabled")
    void notStartIfDisabled() {
        properties.setEnabled(false);
        MetricsCollector collector = new MetricsCollector(properties);

        collector.start();

        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should not start if no targets")
    void notStartIfNoTargets() {
        properties.setTargets(List.of());
        MetricsCollector collector = new MetricsCollector(properties);

        collector.start();

        assertFalse(collector.isRunning());
    }

    @Test
    @DisplayName("Should scrape and buffer metrics")
    void scrapeAndBuffer() throws Exception {
        String prometheusData = """
            test_metric_1 1
            test_metric_2 2
            """;
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        // Don't flush immediately
        properties.setFlushThreshold(100);
        properties.setFlushIntervalMs(5000);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for at least one scrape
        Thread.sleep(80);

        assertTrue(collector.getBufferSize() > 0, "Buffer should contain scraped metrics");

        collector.stop();
    }

    @Test
    @DisplayName("Should flush on threshold")
    void flushOnThreshold() throws Exception {
        // Each scrape returns 3 metrics
        String prometheusData = """
            metric_1 1
            metric_2 2
            metric_3 3
            """;
        // Queue enough responses
        for (int i = 0; i < 10; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }

        properties.setScrapeIntervalMs(20);
        properties.setFlushThreshold(5);
        properties.setFlushIntervalMs(5000); // Long interval so threshold triggers first

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for scrapes and flush
        Thread.sleep(150);

        // Metrics should have been sent (queued to HttpProducer)
        assertTrue(collector.getMetricsSentCount() > 0, "Should have sent metrics");

        collector.stop();
    }

    @Test
    @DisplayName("Should flush on interval")
    void flushOnInterval() throws Exception {
        String prometheusData = "single_metric 1\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        properties.setScrapeIntervalMs(20);
        properties.setFlushThreshold(1000); // High threshold so interval triggers first
        properties.setFlushIntervalMs(50);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for interval to elapse
        Thread.sleep(100);

        collector.stop();

        // Should have sent metrics due to interval
        assertTrue(collector.getMetricsSentCount() > 0, "Should have sent metrics on interval");
    }

    @Test
    @DisplayName("Should flush on stop")
    void flushOnStop() throws Exception {
        String prometheusData = "test_metric 42\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        properties.setFlushThreshold(1000);
        properties.setFlushIntervalMs(5000);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for scrape
        Thread.sleep(80);

        int bufferBefore = collector.getBufferSize();
        assertTrue(bufferBefore > 0, "Buffer should have metrics before stop");

        collector.stop();

        // Buffer should be flushed on stop
        assertEquals(0, collector.getBufferSize(), "Buffer should be empty after stop");
    }

    @Test
    @DisplayName("Should track metrics sent count")
    void trackMetricsSentCount() throws Exception {
        String prometheusData = """
            metric_1 1
            metric_2 2
            """;
        for (int i = 0; i < 5; i++) {
            targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));
        }

        properties.setFlushThreshold(2);
        properties.setFlushIntervalMs(50);

        MetricsCollector collector = new MetricsCollector(properties);
        assertEquals(0, collector.getMetricsSentCount());

        collector.start();

        // Wait for scrapes and flushes
        Thread.sleep(100);

        collector.stop();

        assertTrue(collector.getMetricsSentCount() > 0, "Should have sent metrics");
    }

    @Test
    @DisplayName("Should use target prefix")
    void useTargetPrefix() throws Exception {
        String prometheusData = "prefixed_metric 123\n";
        targetServer.enqueue(new MockResponse().setBody(prometheusData).setResponseCode(200));

        properties.setTargetPrefix(targetServer.url("/").toString().replaceAll("/$", ""));
        properties.setTargets(List.of("/metrics"));
        properties.setFlushThreshold(1);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        Thread.sleep(80);

        collector.stop();

        assertTrue(targetServer.getRequestCount() > 0, "Should have scraped from prefixed target");
    }

    @Test
    @DisplayName("Should handle scrape failures gracefully")
    void handleScrapeFailure() throws Exception {
        // First request fails, second succeeds
        targetServer.enqueue(new MockResponse().setResponseCode(500).setBody("Error"));
        targetServer.enqueue(new MockResponse().setBody("recovered_metric 1\n").setResponseCode(200));

        properties.setFlushThreshold(100);
        properties.setFlushIntervalMs(5000);

        MetricsCollector collector = new MetricsCollector(properties);
        collector.start();

        // Wait for both scrape attempts
        Thread.sleep(100);

        // Should still be running despite the failure
        assertTrue(collector.isRunning(), "Should continue running after scrape failure");

        collector.stop();
    }
}
