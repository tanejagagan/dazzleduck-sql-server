package io.dazzleduck.sql.otel.collector.query;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class QuerySettingsTest {

    private static QuerySettings parse(String hocon) {
        var config = ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve();
        return new CollectorConfig(config).getQuerySettings();
    }

    @Test
    void theBundledDefaultIsDisabledAndLocalhost() {
        var settings = parse("");
        assertFalse(settings.enabled());
        assertEquals("127.0.0.1", settings.host());
        assertEquals(8082, settings.port());
        assertEquals(4, settings.threads());
        assertEquals(Duration.ofSeconds(30), settings.timeout());
        assertEquals(10_000, settings.arrowBatchSize());
    }

    @Test
    void valuesAreReadAndReachTheServerProperties() {
        var config = ConfigFactory.parseString("""
                otel_collector.query { enabled = true, port = 9000, threads = 2, timeout = 5 seconds, arrow_batch_size = 500 }
                """).withFallback(ConfigFactory.load()).resolve();
        var settings = new CollectorConfig(config).toProperties().getQuerySettings();
        assertTrue(settings.enabled());
        assertEquals(9000, settings.port());
        assertEquals(2, settings.threads());
        assertEquals(Duration.ofSeconds(5), settings.timeout());
        assertEquals(500, settings.arrowBatchSize());
    }

    @Test
    void invalidValuesFail() {
        assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.query.port = 70000"));
        assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.query.threads = 0"));
        assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.query.timeout = 0 seconds"));
        assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.query.host = \"\""));
        assertThrows(ConfigException.WrongType.class, () -> parse("otel_collector.query.enabled = ture"));
    }
}
