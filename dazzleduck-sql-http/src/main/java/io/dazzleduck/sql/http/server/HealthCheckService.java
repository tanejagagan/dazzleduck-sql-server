package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.helidon.http.Status;
import io.helidon.webserver.http.*;

import java.sql.Connection;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HealthCheckService implements HttpService {

    private static final Logger LOGGER = Logger.getLogger(HealthCheckService.class.getName());

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Instant startTime = Instant.now();

    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong bytesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();

    @Override
    public void routing(HttpRules rules) {rules.get("/", this::health);}

    private void health(ServerRequest req, ServerResponse res) {
        trackRequest(req);

        boolean dbUp;
        String dbError = null;

        try (Connection conn = ConnectionPool.getConnection();
             var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT 1")) {

            dbUp = rs.next();

            if (!dbUp) {
                dbError = "SELECT 1 returned no rows";
            }

        } catch (Exception e) {
            dbUp = false;
            dbError = e.getMessage();
            LOGGER.log(Level.WARNING, "Database health check failed", e);
        }

        Map<String, Object> body = Map.of(
                "status", dbUp ? "UP" : "DEGRADED",
                "uptime_seconds", uptimeSeconds(),
                "database", dbUp ? Map.of("status", "UP", "check", "SELECT 1") : Map.of("status", "DOWN", "check", "SELECT 1", "error", dbError),
                "metrics", Map.of("requests_total", requestCount.get(), "bytes_in", bytesIn.get(), "bytes_out", bytesOut.get()),
                "timestamp", Instant.now().toString()
        );

        send(res, dbUp ? Status.OK_200 : Status.SERVICE_UNAVAILABLE_503, body);
    }

    private void trackRequest(ServerRequest req) {
        requestCount.incrementAndGet();
        bytesIn.addAndGet(req.headers().size() * 50L);
        req.headers().contentLength().ifPresent(bytesIn::addAndGet);
    }

    private long uptimeSeconds() {
        return Instant.now().getEpochSecond() - startTime.getEpochSecond();
    }

    private void send(ServerResponse res, Status status, Map<String, Object> body) {
        try {
            String json = MAPPER.writeValueAsString(body);
            bytesOut.addAndGet(json.length());
            res.status(status)
                    .header("Content-Type", "application/json")
                    .send(json);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Health response serialization failed", e);
            res.status(Status.INTERNAL_SERVER_ERROR_500)
                    .send("{\"error\":\"internal_error\"}");
        }
    }
}
