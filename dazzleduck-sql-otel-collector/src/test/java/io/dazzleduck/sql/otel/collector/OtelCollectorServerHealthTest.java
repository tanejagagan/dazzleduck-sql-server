package io.dazzleduck.sql.otel.collector;

import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.dazzleduck.sql.otel.collector.health.CollectorHealthStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the otel-collector's health status walks HEALTHY -&gt; MAINTENANCE -&gt; DOWN across its
 * start/close lifecycle, and that the HTTP {@code /health} endpoint reflects each state.
 */
class OtelCollectorServerHealthTest {

    static final String SECRET_KEY_BASE64 =
            "VGhpcyBpcyBhIDY0IGJpdCBsb25nIGtleSB3aGljaCBzaG91bGQgYmUgY2hhbmdlZCBpbiBwcm9kdWN0aW9uLiBTbyBjaGFuZ2UgbWUgYW5kIG1ha2Ugc3VyZSBpdHMgMTI4IGJpdCBsb25nIG9yIG1vcmU";

    @Test
    void healthWalksHealthyMaintenanceDownAcrossLifecycle() throws Exception {
        CollectorProperties props = new CollectorProperties();
        props.setGrpcPort(freePort());
        props.setHealthPort(freePort());
        props.setAuthentication("jwt");
        props.setSecretKey(SECRET_KEY_BASE64);
        // Small grace so the MAINTENANCE window is observable mid-sleep without slowing the test.
        props.setShutdownGracePeriod(Duration.ofMillis(300));

        OtelCollectorServer server = new OtelCollectorServer(props);
        try {
            server.start();
            assertEquals(CollectorHealthStatus.HEALTHY, server.getHealthStatus());
            assertEquals(200, getHealth(props.getHealthPort()).statusCode());

            Thread closer = new Thread(server::close, "test-closer");
            closer.start();
            // Mid-grace-period (300ms): should already be draining, well before close() returns.
            Thread.sleep(100);
            assertEquals(CollectorHealthStatus.MAINTENANCE, server.getHealthStatus());
            assertEquals(503, getHealth(props.getHealthPort()).statusCode());

            closer.join(Duration.ofSeconds(10).toMillis());
            assertEquals(CollectorHealthStatus.DOWN, server.getHealthStatus());
        } finally {
            server.close();
        }
    }

    private static HttpResponse<String> getHealth(int port) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/health"))
                .GET().build();
        return HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws IOException {
        try (var s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }
}
