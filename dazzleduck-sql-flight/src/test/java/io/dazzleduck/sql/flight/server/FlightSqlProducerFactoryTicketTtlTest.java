package io.dazzleduck.sql.flight.server;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigConstants;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/** ticket_ttl_ms reaches the producer, for every access mode, and a bad value fails before anything is built. */
class FlightSqlProducerFactoryTicketTtlTest {

    private static Config config(String overrides) {
        return ConfigFactory.parseString(overrides)
                .withFallback(ConfigFactory.parseResources("reference.conf"))
                .withFallback(ConfigFactory.systemProperties())
                .resolve()
                .getConfig(ConfigConstants.CONFIG_PATH);
    }

    @Test
    void configuredTtlReachesTheProducerInEveryAccessMode() {
        for (String mode : new String[]{"COMPLETE", "READ_ONLY", "RESTRICTED", "RESTRICT_READ_ONLY"}) {
            try (var producer = FlightSqlProducerFactory.builder(config(
                    "dazzleduck_server { ticket_ttl_ms = 300000, access_mode = " + mode + " }")).build()) {
                assertEquals(Duration.ofMinutes(5), producer.getTicketTtl(), mode);
            } catch (Exception e) {
                fail(mode, e);
            }
        }
    }

    @Test
    void defaultIsOneHour() throws Exception {
        try (var producer = FlightSqlProducerFactory.builder(config("")).build()) {
            assertEquals(Duration.ofHours(1), producer.getTicketTtl());
        }
    }

    @Test
    void nonPositiveTtlIsRejectedWhenTheConfigIsRead() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> FlightSqlProducerFactory.builder(config("dazzleduck_server.ticket_ttl_ms = 0")));
        assertTrue(e.getMessage().contains("ticket_ttl_ms"), e.getMessage());
    }
}
