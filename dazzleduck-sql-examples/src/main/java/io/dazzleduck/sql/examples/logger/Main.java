package io.dazzleduck.sql.examples.logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Professional continuous logging demo application for DazzleDuck Logger.
 *
 * <p>This application generates realistic log messages at configurable intervals
 * using the standard SLF4J API. The dazzleduck-sql-logger library acts as the
 * SLF4J provider and automatically sends logs to the DazzleDuck server.</p>
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Continuous log generation at 1-second intervals</li>
 *   <li>Multiple log levels (TRACE, DEBUG, INFO, WARN, ERROR)</li>
 *   <li>MDC context support for request tracing</li>
 *   <li>Marker-based log categorization</li>
 *   <li>Simulated real-world scenarios</li>
 *   <li>Graceful shutdown handling</li>
 *   <li>Statistics tracking</li>
 * </ul>
 *
 * @author DazzleDuck Team
 * @version 1.0.0
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");
    private static final Logger performanceLog = LoggerFactory.getLogger("PERFORMANCE");
    private static final Logger securityLog = LoggerFactory.getLogger("SECURITY");

    // Markers for log categorization
    private static final Marker AUDIT = MarkerFactory.getMarker("AUDIT");
    private static final Marker PERFORMANCE = MarkerFactory.getMarker("PERFORMANCE");
    private static final Marker SECURITY = MarkerFactory.getMarker("SECURITY");
    private static final Marker DATABASE = MarkerFactory.getMarker("DATABASE");
    private static final Marker API = MarkerFactory.getMarker("API");
    private static final Marker BUSINESS = MarkerFactory.getMarker("BUSINESS");

    // Configuration
    private static final long LOG_INTERVAL_MS = 1000; // 1 second
    private static final String DEFAULT_SERVER_URL = "http://dazzleduck-server:8081";

    // State management
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong totalLogsGenerated = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);
    private static final Random random = new Random();
    private static Instant startTime;

    // Simulated data
    private static final String[] USERS = {
            "john.doe", "jane.smith", "bob.wilson", "alice.johnson", "charlie.brown",
            "diana.ross", "edward.norton", "fiona.apple", "george.lucas", "helen.troy"
    };

    private static final String[] ACTIONS = {
            "LOGIN", "LOGOUT", "VIEW_DASHBOARD", "UPDATE_PROFILE", "CREATE_ORDER",
            "DELETE_ITEM", "SEARCH", "EXPORT_DATA", "IMPORT_DATA", "GENERATE_REPORT"
    };

    private static final String[] ENDPOINTS = {
            "/api/v1/users", "/api/v1/orders", "/api/v1/products", "/api/v1/reports",
            "/api/v1/analytics", "/api/v1/settings", "/api/v1/notifications", "/api/v1/auth"
    };

    private static final String[] HTTP_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"};

    private static final String[] DATABASE_OPERATIONS = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "JOIN", "AGGREGATE"
    };

    private static final String[] TABLES = {
            "users", "orders", "products", "inventory", "transactions", "audit_log"
    };

    public static void main(String[] args) {
        printBanner();
        setupShutdownHook();
        startTime = Instant.now();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "LogGenerator");
            t.setDaemon(false);
            return t;
        });

        log.info("DazzleDuck Logging Demo started - generating logs every {} ms", LOG_INTERVAL_MS);

        // Schedule continuous log generation
        scheduler.scheduleAtFixedRate(
                Main::generateLogBatch,
                0,
                LOG_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        // Schedule statistics printing every 30 seconds
        scheduler.scheduleAtFixedRate(
                Main::printStatistics,
                30,
                30,
                TimeUnit.SECONDS
        );

        // Keep the main thread alive
        try {
            while (running.get()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Main thread interrupted");
        } finally {
            shutdown(scheduler);
        }
    }

    /**
     * Generates a batch of logs simulating real application behavior.
     */
    private static void generateLogBatch() {
        if (!running.get()) {
            return;
        }

        try {
            String requestId = generateRequestId();
            String sessionId = generateSessionId();
            String user = getRandomUser();

            // Set MDC context for request tracing
            MDC.put("request_id", requestId);
            MDC.put("session_id", sessionId);
            MDC.put("user_id", user);
            MDC.put("instance_id", "app-instance-01");

            try {
                // Randomly select scenario type
                int scenario = random.nextInt(100);

                if (scenario < 40) {
                    // 40% - Normal API request flow
                    simulateApiRequest(user);
                } else if (scenario < 60) {
                    // 20% - Database operation
                    simulateDatabaseOperation();
                } else if (scenario < 75) {
                    // 15% - User action audit
                    simulateUserAction(user);
                } else if (scenario < 85) {
                    // 10% - Performance monitoring
                    simulatePerformanceLog();
                } else if (scenario < 95) {
                    // 10% - Security event
                    simulateSecurityEvent(user);
                } else {
                    // 5% - Error scenario
                    simulateErrorScenario();
                }

                totalLogsGenerated.incrementAndGet();

            } finally {
                MDC.clear();
            }

        } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Error generating log batch", e);
        }
    }

    /**
     * Simulates an API request with timing and response logging.
     */
    private static void simulateApiRequest(String user) {
        String endpoint = getRandomEndpoint();
        String method = getRandomHttpMethod();
        int responseTime = random.nextInt(500) + 10;
        int statusCode = getRandomStatusCode();

        log.info(API, "Incoming {} request to {} from user {}", method, endpoint, user);

        if (log.isDebugEnabled()) {
            log.debug(API, "Request headers validated for {}", endpoint);
            log.debug(API, "Authentication token verified for user {}", user);
        }

        if (responseTime > 300) {
            log.warn(API, "Slow response detected: {} {} completed in {}ms (threshold: 300ms)",
                    method, endpoint, responseTime);
        } else {
            log.info(API, "{} {} completed with status {} in {}ms",
                    method, endpoint, statusCode, responseTime);
        }

        if (statusCode >= 400 && statusCode < 500) {
            log.warn(API, "Client error on {} {}: status {}", method, endpoint, statusCode);
        } else if (statusCode >= 500) {
            log.error(API, "Server error on {} {}: status {}", method, endpoint, statusCode);
        }
    }

    /**
     * Simulates database operations with query timing.
     */
    private static void simulateDatabaseOperation() {
        String operation = getRandomDatabaseOperation();
        String table = getRandomTable();
        int queryTime = random.nextInt(200) + 5;
        int rowsAffected = random.nextInt(1000) + 1;

        log.debug(DATABASE, "Executing {} query on table {}", operation, table);

        if (queryTime > 100) {
            log.warn(DATABASE, "Slow query detected: {} on {} took {}ms, {} rows affected",
                    operation, table, queryTime, rowsAffected);
        } else {
            log.info(DATABASE, "{} on {} completed in {}ms, {} rows affected",
                    operation, table, queryTime, rowsAffected);
        }

        if (random.nextInt(100) < 5) {
            log.warn(DATABASE, "Connection pool utilization high: {}%", random.nextInt(30) + 70);
        }
    }

    /**
     * Simulates user action audit logging.
     */
    private static void simulateUserAction(String user) {
        String action = getRandomAction();
        String ipAddress = generateIpAddress();

        MDC.put("ip_address", ipAddress);
        MDC.put("action", action);

        log.info(AUDIT, "User {} performed action: {} from IP {}", user, action, ipAddress);

        if (action.equals("LOGIN")) {
            log.info(AUDIT, "New session created for user {}", user);
        } else if (action.equals("LOGOUT")) {
            log.info(AUDIT, "Session terminated for user {}", user);
        } else if (action.equals("UPDATE_PROFILE")) {
            log.info(AUDIT, "Profile updated for user {}: fields modified", user);
        }
    }

    /**
     * Simulates performance monitoring logs.
     */
    private static void simulatePerformanceLog() {
        int cpuUsage = random.nextInt(100);
        int memoryUsage = random.nextInt(100);
        int activeConnections = random.nextInt(500);
        int requestsPerSecond = random.nextInt(1000) + 100;

        log.info(PERFORMANCE, "System metrics: CPU={}%, Memory={}%, Connections={}, RPS={}",
                cpuUsage, memoryUsage, activeConnections, requestsPerSecond);

        if (cpuUsage > 80) {
            log.warn(PERFORMANCE, "High CPU usage detected: {}%", cpuUsage);
        }

        if (memoryUsage > 85) {
            log.warn(PERFORMANCE, "High memory usage detected: {}%", memoryUsage);
        }

        if (log.isDebugEnabled()) {
            log.debug(PERFORMANCE, "GC stats: Young gen collections={}, Old gen collections={}",
                    random.nextInt(100), random.nextInt(10));
        }
    }

    /**
     * Simulates security-related events.
     */
    private static void simulateSecurityEvent(String user) {
        int eventType = random.nextInt(100);
        String ipAddress = generateIpAddress();

        MDC.put("ip_address", ipAddress);
        MDC.put("security_level", eventType < 80 ? "INFO" : "WARNING");

        if (eventType < 60) {
            log.info(SECURITY, "Successful authentication for user {} from {}", user, ipAddress);
        } else if (eventType < 80) {
            log.info(SECURITY, "Password changed for user {}", user);
        } else if (eventType < 90) {
            log.warn(SECURITY, "Failed login attempt for user {} from {}", user, ipAddress);
        } else if (eventType < 95) {
            log.warn(SECURITY, "Suspicious activity detected for user {} - multiple failed attempts",
                    user);
        } else {
            log.warn(SECURITY, "Rate limit exceeded for IP {}", ipAddress);
        }
    }

    /**
     * Simulates error scenarios with stack traces.
     */
    private static void simulateErrorScenario() {
        int errorType = random.nextInt(5);

        try {
            switch (errorType) {
                case 0:
                    throw new IllegalArgumentException("Invalid parameter: userId cannot be null");
                case 1:
                    throw new IllegalStateException("Service temporarily unavailable");
                case 2:
                    try {
                        throw new java.sql.SQLException("Connection timeout after 30000ms");
                    } catch (Exception inner) {
                        throw new RuntimeException("Database operation failed", inner);
                    }
                case 3:
                    throw new java.util.concurrent.TimeoutException("Request timed out after 60 seconds");
                case 4:
                    throw new SecurityException("Access denied: insufficient permissions");
                default:
                    throw new RuntimeException("Unexpected error occurred");
            }
        } catch (Exception e) {
            log.error("Error occurred during request processing: {}", e.getMessage(), e);
            errorCount.incrementAndGet();
        }
    }

    /**
     * Prints current statistics about log generation.
     */
    private static void printStatistics() {
        Duration uptime = Duration.between(startTime, Instant.now());
        long totalLogs = totalLogsGenerated.get();
        long errors = errorCount.get();
        double logsPerSecond = totalLogs / Math.max(1, uptime.getSeconds());

        System.out.println();
        System.out.println("┌─────────────────────────────────────────────────┐");
        System.out.println("│           DazzleDuck Logger Statistics          │");
        System.out.println("├─────────────────────────────────────────────────┤");
        System.out.printf("│  Uptime:           %26s  │%n", formatDuration(uptime));
        System.out.printf("│  Total Logs:       %,26d  │%n", totalLogs);
        System.out.printf("│  Errors:           %,26d  │%n", errors);
        System.out.printf("│  Logs/Second:      %26.2f  │%n", logsPerSecond);
        System.out.printf("│  Timestamp:        %26s  │%n",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("└─────────────────────────────────────────────────┘");
        System.out.println();

        log.info("Statistics: uptime={}, totalLogs={}, errors={}, logsPerSecond={:.2f}",
                formatDuration(uptime), totalLogs, errors, logsPerSecond);
    }

    /**
     * Sets up graceful shutdown handling.
     */
    private static void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("Shutdown signal received...");
            running.set(false);
            log.info("Application shutdown initiated");
        }, "ShutdownHook"));
    }

    /**
     * Performs graceful shutdown of the scheduler.
     */
    private static void shutdown(ScheduledExecutorService scheduler) {
        System.out.println("Shutting down scheduler...");
        log.info("Shutting down log generator");

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                log.warn("Scheduler did not terminate gracefully, forcing shutdown");
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final statistics
        printStatistics();

        System.out.println();
        System.out.println("============================================");
        System.out.println("    DazzleDuck Logging Demo Stopped");
        System.out.println("============================================");
        System.out.println();
        System.out.println("Query your logs:");
        System.out.println("  SELECT * FROM loglake.main.log ORDER BY timestamp DESC LIMIT 100;");
        System.out.println();

        log.info("Application shutdown complete. Total logs generated: {}", totalLogsGenerated.get());
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
        System.out.println("║                   DAZZLE-DUCK LOGGING DEMO v1.0.0             ║");
        System.out.println("║                                                               ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Server:     " + String.format("%-49s", DEFAULT_SERVER_URL) + "║");
        System.out.println("║  Interval:   " + String.format("%-49s", LOG_INTERVAL_MS + " ms") + "║");
        System.out.println("║  Press Ctrl+C to stop                                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    // ======================== Helper Methods ========================

    private static String generateRequestId() {
        return "REQ-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private static String generateSessionId() {
        return "SESS-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
    }

    private static String generateIpAddress() {
        return String.format("%d.%d.%d.%d",
                random.nextInt(223) + 1,
                random.nextInt(256),
                random.nextInt(256),
                random.nextInt(254) + 1);
    }

    private static String getRandomUser() {
        return USERS[random.nextInt(USERS.length)];
    }

    private static String getRandomAction() {
        return ACTIONS[random.nextInt(ACTIONS.length)];
    }

    private static String getRandomEndpoint() {
        return ENDPOINTS[random.nextInt(ENDPOINTS.length)];
    }

    private static String getRandomHttpMethod() {
        return HTTP_METHODS[random.nextInt(HTTP_METHODS.length)];
    }

    private static String getRandomDatabaseOperation() {
        return DATABASE_OPERATIONS[random.nextInt(DATABASE_OPERATIONS.length)];
    }

    private static String getRandomTable() {
        return TABLES[random.nextInt(TABLES.length)];
    }

    private static int getRandomStatusCode() {
        int rand = random.nextInt(100);
        if (rand < 85) return 200;          // 85% success
        if (rand < 90) return 201;          // 5% created
        if (rand < 93) return 400;          // 3% bad request
        if (rand < 96) return 401;          // 3% unauthorized
        if (rand < 98) return 404;          // 2% not found
        return 500;                          // 2% server error
    }

    private static String formatDuration(Duration duration) {
        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }
}