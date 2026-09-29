package io.dazzleduck.sql.otel.collector.compaction;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompactionSettingsTest {

    private static CompactionSettings parse(String hocon) {
        var config = ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve();
        return new CollectorConfig(config).getCompactionSettings();
    }

    @Test
    void theBundledDefaultIsDisabled() {
        var settings = parse("");
        assertFalse(settings.enabled());
        assertEquals(List.of(), settings.catalogs());
        assertEquals(Duration.ofMinutes(1), settings.minorFrequency());
        assertEquals(8_000_000L, settings.minorMaxFileSize(), "HOCON's MB is decimal");
        assertEquals(Duration.ofHours(1), settings.majorFrequency());
        assertEquals(Duration.ofMinutes(15), settings.snapshotRetention());
        assertNull(settings.rewriteDeleteThreshold());
        assertFalse(settings.orphanCleanupEnabled());
        assertEquals(Duration.ofDays(2), settings.orphanOlderThan());
    }

    @Test
    void allValuesAreRead() {
        var settings = parse("""
                otel_collector.compaction {
                    enabled = true
                    catalogs = [lake, other_lake]
                    minor { frequency = 30 seconds, max_file_size = 4MB }
                    major { frequency = 2 hours, snapshot_retention = 1 hour, rewrite_delete_threshold = 0.3 }
                    orphan_cleanup { enabled = true, frequency = 12 hours, older_than = 3 days }
                }
                """);
        assertTrue(settings.enabled());
        assertEquals(List.of("lake", "other_lake"), settings.catalogs());
        assertEquals(Duration.ofSeconds(30), settings.minorFrequency());
        assertEquals(4_000_000L, settings.minorMaxFileSize());
        assertEquals(Duration.ofHours(2), settings.majorFrequency());
        assertEquals(Duration.ofHours(1), settings.snapshotRetention());
        assertEquals(0.3, settings.rewriteDeleteThreshold());
        assertTrue(settings.orphanCleanupEnabled());
        assertEquals(Duration.ofHours(12), settings.orphanFrequency());
        assertEquals(Duration.ofDays(3), settings.orphanOlderThan());
    }

    @Test
    void enabledWithoutCatalogsFails() {
        var e = assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.compaction.enabled = true"));
        assertTrue(e.getMessage().contains("catalogs"), e.getMessage());
    }

    @Test
    void anOrphanAgeUnderAnHourFails() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> parse("otel_collector.compaction.orphan_cleanup.older_than = 30 minutes"));
        assertTrue(e.getMessage().contains("older_than"), e.getMessage());
    }

    @Test
    void invalidValuesFail() {
        assertThrows(IllegalArgumentException.class,
                () -> parse("otel_collector.compaction.major.rewrite_delete_threshold = 1.5"));
        assertThrows(IllegalArgumentException.class, () -> parse("otel_collector.compaction.minor.frequency = 0 seconds"));
        assertThrows(IllegalArgumentException.class,
                () -> parse("otel_collector.compaction { enabled = true, catalogs = [\"lake'; DROP\"] }"));
        assertThrows(ConfigException.WrongType.class, () -> parse("otel_collector.compaction.enabled = ture"));
        assertThrows(ConfigException.BadValue.class, () -> parse("otel_collector.compaction.minor.frequency = soon"));
    }

    @Test
    void settingsReachTheServerProperties() {
        var config = ConfigFactory.parseString("otel_collector.compaction { enabled = true, catalogs = [lake] }")
                .withFallback(ConfigFactory.load()).resolve();
        var props = new CollectorConfig(config).toProperties();
        assertTrue(props.getCompactionSettings().enabled());
        assertEquals(List.of("lake"), props.getCompactionSettings().catalogs());
    }
}
