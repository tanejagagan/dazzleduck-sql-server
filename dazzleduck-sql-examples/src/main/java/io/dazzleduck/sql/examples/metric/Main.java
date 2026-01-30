package io.dazzleduck.sql.examples.metric;

import io.dazzleduck.sql.micrometer.MicrometerForwarder;
import io.dazzleduck.sql.micrometer.config.MicrometerForwarderConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Professional continuous metrics demo application for DazzleDuck Micrometer Forwarder.
 *
 * <p>This application generates realistic metrics at 1-second intervals using
 * the Micrometer API. The dazzleduck-sql-micrometer library forwards metrics
 * to the DazzleDuck server automatically.</p>
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Continuous metrics generation at 1-second intervals</li>
 *   <li>Multiple metric types (Counters, Gauges, Timers, Distribution Summaries)</li>
 *   <li>Simulated real-world application metrics</li>
 *   <li>Graceful shutdown handling</li>
 *   <li>Statistics tracking</li>
 * </ul>
 *
 * @author DazzleDuck Team
 * @version 1.0.0
 */
public class Main {

    // Configuration
    private static final String SERVER_URL = "http://dazzleduck-server:8081";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private static final long METRIC_INTERVAL_MS = 1000; // 1 second
    private static final Duration STEP_INTERVAL = Duration.ofSeconds(1);

    // Application metadata
    private static final String APPLICATION_ID = "java-metrics-demo";
    private static final String APPLICATION_NAME = "DazzleDuck Metrics Demo";
    private static final String APPLICATION_HOST = "localhost";

    // State management
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong iterationCount = new AtomicLong(0);
    private static final Random random = new Random();
    private static Instant startTime;

    // Micrometer components
    private static MicrometerForwarder forwarder;
    private static CompositeMeterRegistry registry;

    // Gauge backing values
    private static final AtomicLong activeConnections = new AtomicLong(0);
    private static final AtomicLong queueSize = new AtomicLong(0);
    private static final AtomicLong cacheSize = new AtomicLong(0);
    private static final AtomicLong threadPoolActive = new AtomicLong(0);
    private static final AtomicLong memoryUsedMb = new AtomicLong(0);
    private static final AtomicLong cpuUsagePercent = new AtomicLong(0);

    // Counters
    private static Counter httpRequestsGet;
    private static Counter httpRequestsPost;
    private static Counter httpRequestsPut;
    private static Counter httpRequestsDelete;
    private static Counter httpErrors4xx;
    private static Counter httpErrors5xx;
    private static Counter dbQueriesSelect;
    private static Counter dbQueriesInsert;
    private static Counter dbQueriesUpdate;
    private static Counter dbQueriesDelete;
    private static Counter cacheHits;
    private static Counter cacheMisses;
    private static Counter messagesReceived;
    private static Counter messagesSent;
    private static Counter eventsProcessed;

    // Timers
    private static Timer httpResponseTime;
    private static Timer dbQueryTime;
    private static Timer cacheAccessTime;
    private static Timer messageProcessingTime;

    // Distribution Summaries
    private static DistributionSummary requestPayloadSize;
    private static DistributionSummary responsePayloadSize;

    public static void main(String[] args) {
        printBanner();
        setupShutdownHook();
        startTime = Instant.now();

        try {
            // Initialize metrics pipeline
            System.out.println("Initializing metrics pipeline...");
            initializeMetricsForwarder();
            registerMetrics();
            System.out.println("Metrics pipeline initialized successfully");
            System.out.println();

            printRegisteredMetrics();

            // Create scheduler for continuous metrics generation
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "MetricsGenerator");
                t.setDaemon(false);
                return t;
            });

            // Schedule continuous metrics generation every 1 second
            scheduler.scheduleAtFixedRate(
                    Main::generateMetrics,
                    0,
                    METRIC_INTERVAL_MS,
                    TimeUnit.MILLISECONDS
            );

            // Schedule statistics printing every 30 seconds
            scheduler.scheduleAtFixedRate(
                    Main::printStatistics,
                    30,
                    30,
                    TimeUnit.SECONDS
            );

            System.out.println("Metrics generation started - publishing every " + METRIC_INTERVAL_MS + " ms");
            System.out.println();

            // Keep the main thread alive
            while (running.get()) {
                Thread.sleep(1000);
            }

            // Shutdown
            shutdown(scheduler);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrupted, shutting down...");
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Initializes the MicrometerForwarder with configuration.
     */
    private static void initializeMetricsForwarder() {
        MicrometerForwarderConfig config = MicrometerForwarderConfig.builder()
                .baseUrl(SERVER_URL)
                .username(USERNAME)
                .password(PASSWORD)
                .claims(Map.of())
                .targetPath("metric")
                .httpClientTimeout(Duration.ofSeconds(10))

                // Publish every 1 second
                .stepInterval(STEP_INTERVAL)
                .minBatchSize(1048576)              // 1 MB
                .maxBatchSize(8 * 1024 * 1024)      // 8 MB
                .maxSendInterval(Duration.ofSeconds(1))

                .maxInMemorySize(10 * 1024 * 1024)  // 10 MB
                .maxOnDiskSize(1024 * 1024 * 1024L) // 1 GB

                .retryCount(3)
                .retryIntervalMillis(1000)

                // Project with application metadata
                .project(List.of(
                        "*",
                        "'" + APPLICATION_HOST + "' AS application_host",
                        "'" + APPLICATION_ID + "' AS application_id",
                        "'" + APPLICATION_NAME + "' AS application_name",
                        "CAST(timestamp AS DATE) AS date"
                ))
                .partitionBy(List.of("date"))

                .enabled(true)
                .build();

        forwarder = new MicrometerForwarder(config);
        forwarder.start();
        registry = forwarder.getRegistry();
    }

    /**
     * Registers all metrics with the registry.
     */
    private static void registerMetrics() {
        // HTTP Request Counters
        httpRequestsGet = registry.counter("http.requests.total", "method", "GET", "endpoint", "/api");
        httpRequestsPost = registry.counter("http.requests.total", "method", "POST", "endpoint", "/api");
        httpRequestsPut = registry.counter("http.requests.total", "method", "PUT", "endpoint", "/api");
        httpRequestsDelete = registry.counter("http.requests.total", "method", "DELETE", "endpoint", "/api");

        // HTTP Error Counters
        httpErrors4xx = registry.counter("http.errors.total", "status_class", "4xx");
        httpErrors5xx = registry.counter("http.errors.total", "status_class", "5xx");

        // Database Query Counters
        dbQueriesSelect = registry.counter("db.queries.total", "operation", "SELECT");
        dbQueriesInsert = registry.counter("db.queries.total", "operation", "INSERT");
        dbQueriesUpdate = registry.counter("db.queries.total", "operation", "UPDATE");
        dbQueriesDelete = registry.counter("db.queries.total", "operation", "DELETE");

        // Cache Counters
        cacheHits = registry.counter("cache.requests.total", "result", "hit");
        cacheMisses = registry.counter("cache.requests.total", "result", "miss");

        // Messaging Counters
        messagesReceived = registry.counter("messaging.messages.total", "direction", "received");
        messagesSent = registry.counter("messaging.messages.total", "direction", "sent");
        eventsProcessed = registry.counter("events.processed.total", "type", "business");

        // Timers
        httpResponseTime = registry.timer("http.response.time", "endpoint", "/api");
        dbQueryTime = registry.timer("db.query.time", "database", "primary");
        cacheAccessTime = registry.timer("cache.access.time", "cache", "redis");
        messageProcessingTime = registry.timer("messaging.processing.time", "queue", "main");

        // Distribution Summaries
        requestPayloadSize = registry.summary("http.request.payload.size", "unit", "bytes");
        responsePayloadSize = registry.summary("http.response.payload.size", "unit", "bytes");

        // Gauges - System Resources
        Gauge.builder("system.cpu.usage", cpuUsagePercent, AtomicLong::get)
                .description("CPU usage percentage")
                .baseUnit("percent")
                .register(registry);

        Gauge.builder("system.memory.used", memoryUsedMb, AtomicLong::get)
                .description("Memory used in MB")
                .baseUnit("megabytes")
                .register(registry);

        // Gauges - Connection Pool
        Gauge.builder("db.connections.active", activeConnections, AtomicLong::get)
                .description("Active database connections")
                .register(registry);

        // Gauges - Thread Pool
        Gauge.builder("executor.threads.active", threadPoolActive, AtomicLong::get)
                .description("Active threads in executor pool")
                .register(registry);

        // Gauges - Queue
        Gauge.builder("messaging.queue.size", queueSize, AtomicLong::get)
                .description("Message queue size")
                .register(registry);

        // Gauges - Cache
        Gauge.builder("cache.size", cacheSize, AtomicLong::get)
                .description("Cache size in entries")
                .register(registry);
    }

    /**
     * Generates a batch of metrics simulating real application behavior.
     */
    private static void generateMetrics() {
        if (!running.get()) {
            return;
        }

        try {
            long iteration = iterationCount.incrementAndGet();

            // Update system resource gauges
            cpuUsagePercent.set(20 + random.nextInt(60));
            memoryUsedMb.set(512 + random.nextInt(1024));
            threadPoolActive.set(5 + random.nextInt(15));

            // Update connection pool gauge
            activeConnections.set(10 + random.nextInt(40));

            // Update queue size gauge
            queueSize.set(random.nextInt(200));

            // Update cache size gauge
            cacheSize.set(5000 + random.nextInt(5000));

            // Simulate HTTP requests
            simulateHttpRequests();

            // Simulate database queries
            simulateDatabaseQueries();

            // Simulate cache operations
            simulateCacheOperations();

            // Simulate messaging
            simulateMessaging();

            // Simulate payload sizes
            simulatePayloads();

            // Print progress every 10 iterations
            if (iteration % 10 == 0) {
                System.out.printf("[%s] Iteration %d - HTTP: %.0f, DB: %.0f, Cache: %.0f/%.0f, Queue: %d%n",
                        LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                        iteration,
                        httpRequestsGet.count() + httpRequestsPost.count() +
                                httpRequestsPut.count() + httpRequestsDelete.count(),
                        dbQueriesSelect.count() + dbQueriesInsert.count() +
                                dbQueriesUpdate.count() + dbQueriesDelete.count(),
                        cacheHits.count(),
                        cacheMisses.count(),
                        queueSize.get());
            }

        } catch (Exception e) {
            System.err.println("Error generating metrics: " + e.getMessage());
        }
    }

    /**
     * Simulates HTTP request metrics.
     */
    private static void simulateHttpRequests() {
        // Simulate various HTTP methods with realistic distribution
        int requestCount = 5 + random.nextInt(15);

        for (int i = 0; i < requestCount; i++) {
            int methodRand = random.nextInt(100);

            if (methodRand < 60) {
                httpRequestsGet.increment();
            } else if (methodRand < 85) {
                httpRequestsPost.increment();
            } else if (methodRand < 95) {
                httpRequestsPut.increment();
            } else {
                httpRequestsDelete.increment();
            }

            // Record response time
            httpResponseTime.record(Duration.ofMillis(20 + random.nextInt(300)));
        }

        // Simulate errors (5% of requests)
        if (random.nextInt(100) < 5) {
            if (random.nextBoolean()) {
                httpErrors4xx.increment();
            } else {
                httpErrors5xx.increment();
            }
        }
    }

    /**
     * Simulates database query metrics.
     */
    private static void simulateDatabaseQueries() {
        int queryCount = 3 + random.nextInt(10);

        for (int i = 0; i < queryCount; i++) {
            int opRand = random.nextInt(100);

            if (opRand < 70) {
                dbQueriesSelect.increment();
            } else if (opRand < 85) {
                dbQueriesInsert.increment();
            } else if (opRand < 95) {
                dbQueriesUpdate.increment();
            } else {
                dbQueriesDelete.increment();
            }

            // Record query time
            dbQueryTime.record(Duration.ofMillis(5 + random.nextInt(100)));
        }
    }

    /**
     * Simulates cache operation metrics.
     */
    private static void simulateCacheOperations() {
        int cacheOps = 10 + random.nextInt(20);
        int hitRate = 80 + random.nextInt(15); // 80-95% hit rate

        for (int i = 0; i < cacheOps; i++) {
            if (random.nextInt(100) < hitRate) {
                cacheHits.increment();
            } else {
                cacheMisses.increment();
            }

            // Record access time
            cacheAccessTime.record(Duration.ofNanos((100L + random.nextInt(2000)) * 1_000));
        }
    }

    /**
     * Simulates messaging metrics.
     */
    private static void simulateMessaging() {
        int received = random.nextInt(10);
        int sent = random.nextInt(8);
        int processed = random.nextInt(15);

        for (int i = 0; i < received; i++) {
            messagesReceived.increment();
        }

        for (int i = 0; i < sent; i++) {
            messagesSent.increment();
        }

        for (int i = 0; i < processed; i++) {
            eventsProcessed.increment();
            messageProcessingTime.record(Duration.ofMillis(10 + random.nextInt(100)));
        }
    }

    /**
     * Simulates payload size metrics.
     */
    private static void simulatePayloads() {
        // Request payloads: 100 bytes to 50KB
        requestPayloadSize.record(100 + random.nextInt(50000));

        // Response payloads: 500 bytes to 100KB
        responsePayloadSize.record(500 + random.nextInt(100000));
    }

    /**
     * Prints registered metrics summary.
     */
    private static void printRegisteredMetrics() {
        System.out.println("┌─────────────────────────────────────────────────┐");
        System.out.println("│              Registered Metrics                 │");
        System.out.println("├─────────────────────────────────────────────────┤");
        System.out.println("│  Counters:                                      │");
        System.out.println("│    - http.requests.total (GET/POST/PUT/DELETE)  │");
        System.out.println("│    - http.errors.total (4xx/5xx)                │");
        System.out.println("│    - db.queries.total (SELECT/INSERT/...)       │");
        System.out.println("│    - cache.requests.total (hit/miss)            │");
        System.out.println("│    - messaging.messages.total (sent/received)   │");
        System.out.println("│    - events.processed.total                     │");
        System.out.println("│                                                 │");
        System.out.println("│  Timers:                                        │");
        System.out.println("│    - http.response.time                         │");
        System.out.println("│    - db.query.time                              │");
        System.out.println("│    - cache.access.time                          │");
        System.out.println("│    - messaging.processing.time                  │");
        System.out.println("│                                                 │");
        System.out.println("│  Gauges:                                        │");
        System.out.println("│    - system.cpu.usage                           │");
        System.out.println("│    - system.memory.used                         │");
        System.out.println("│    - db.connections.active                      │");
        System.out.println("│    - executor.threads.active                    │");
        System.out.println("│    - messaging.queue.size                       │");
        System.out.println("│    - cache.size                                 │");
        System.out.println("│                                                 │");
        System.out.println("│  Distribution Summaries:                        │");
        System.out.println("│    - http.request.payload.size                  │");
        System.out.println("│    - http.response.payload.size                 │");
        System.out.println("└─────────────────────────────────────────────────┘");
        System.out.println();
    }

    /**
     * Prints current statistics.
     */
    private static void printStatistics() {
        Duration uptime = Duration.between(startTime, Instant.now());
        long iterations = iterationCount.get();
        double metricsPerSecond = iterations / Math.max(1, uptime.getSeconds());

        double totalHttpRequests = httpRequestsGet.count() + httpRequestsPost.count() +
                httpRequestsPut.count() + httpRequestsDelete.count();
        double totalDbQueries = dbQueriesSelect.count() + dbQueriesInsert.count() +
                dbQueriesUpdate.count() + dbQueriesDelete.count();
        double totalErrors = httpErrors4xx.count() + httpErrors5xx.count();
        double cacheHitRate = cacheHits.count() /
                Math.max(1, cacheHits.count() + cacheMisses.count()) * 100;

        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║               DazzleDuck Metrics Statistics                   ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Uptime:            %40s  ║%n", formatDuration(uptime));
        System.out.printf("║  Iterations:        %,40d  ║%n", iterations);
        System.out.printf("║  Metrics/Second:    %40.2f  ║%n", metricsPerSecond);
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  HTTP Requests:     %,40.0f  ║%n", totalHttpRequests);
        System.out.printf("║  HTTP Errors:       %,40.0f  ║%n", totalErrors);
        System.out.printf("║  DB Queries:        %,40.0f  ║%n", totalDbQueries);
        System.out.printf("║  Cache Hit Rate:    %39.1f%%  ║%n", cacheHitRate);
        System.out.printf("║  Messages Processed:%,40.0f  ║%n", eventsProcessed.count());
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  CPU Usage:         %39d%%  ║%n", cpuUsagePercent.get());
        System.out.printf("║  Memory Used:       %37d MB  ║%n", memoryUsedMb.get());
        System.out.printf("║  Active Connections:%,40d  ║%n", activeConnections.get());
        System.out.printf("║  Queue Size:        %,40d  ║%n", queueSize.get());
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Timestamp:         %40s  ║%n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * Sets up graceful shutdown handling.
     */
    private static void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("Shutdown signal received...");
            running.set(false);
        }, "ShutdownHook"));
    }

    /**
     * Performs graceful shutdown.
     */
    private static void shutdown(ScheduledExecutorService scheduler) {
        System.out.println("Shutting down metrics generator...");

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final statistics
        printStatistics();

        // Shutdown forwarder
        System.out.println("Flushing remaining metrics...");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (forwarder != null) {
            System.out.println("Closing MicrometerForwarder...");
            forwarder.close();
            System.out.println("MicrometerForwarder closed");
        }

        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║           DazzleDuck Metrics Demo Complete                    ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Query your metrics:");
        System.out.println("  SELECT * FROM loglake.main.metric ORDER BY timestamp DESC LIMIT 100;");
        System.out.println("  SELECT name, COUNT(*) FROM loglake.main.metric GROUP BY name;");
        System.out.println("  SELECT name, AVG(value) FROM loglake.main.metric GROUP BY name;");
        System.out.println();
    }

    /**
     * Prints the application banner.
     */
    private static void printBanner() {
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                               ║");
        System.out.println("║     ██████╗  █████╗ ███████╗███████╗██╗     ███████╗          ║");
        System.out.println("║     ██╔══██╗██╔══██╗╚══███╔╝╚══███╔╝██║     ██╔════╝          ║");
        System.out.println("║     ██║  ██║███████║  ███╔╝   ███╔╝ ██║     █████╗            ║");
        System.out.println("║     ██║  ██║██╔══██║ ███╔╝   ███╔╝  ██║     ██╔══╝            ║");
        System.out.println("║     ██████╔╝██║  ██║███████╗███████╗███████╗███████╗          ║");
        System.out.println("║     ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝╚══════╝          ║");
        System.out.println("║                                                               ║");
        System.out.println("║                  DAZZLE-DUCK METRICS DEMO v1.0.0              ║");
        System.out.println("║                                                               ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Server:        " + String.format("%-46s", SERVER_URL) + "║");
        System.out.println("║  App ID:        " + String.format("%-46s", APPLICATION_ID) + "║");
        System.out.println("║  Interval:      " + String.format("%-46s", METRIC_INTERVAL_MS + " ms") + "║");
        System.out.println("║  Step:          " + String.format("%-46s", STEP_INTERVAL.toSeconds() + " seconds") + "║");
        System.out.println("║                                                               ║");
        System.out.println("║  Press Ctrl+C to stop                                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * Formats a Duration as HH:MM:SS.
     */
    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
}