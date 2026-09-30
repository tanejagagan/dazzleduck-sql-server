package io.dazzleduck.sql.flight.server;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The factory must hand the configured cursor limits to every producer. Previously only COMPLETE
 * got them: the READ_ONLY, RESTRICTED and RESTRICT_READ_ONLY producers were built through a
 * constructor that fell back to {@link CursorConfig#DEFAULT}, so cursor_ttl_ms and the cursor
 * limits could not be changed in those modes.
 */
class FlightSqlProducerFactoryCursorConfigTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @EnumSource(value = AccessMode.class, names = {"COMPLETE", "READ_ONLY", "RESTRICTED", "RESTRICT_READ_ONLY"})
    void everyAccessModeGetsTheConfiguredCursorLimits(AccessMode mode) throws Exception {
        var config = ConfigFactory.parseString("""
                        cursor_ttl_ms = 123456
                        max_cursors_per_identity = 7
                        max_cursors_total = 99
                        warehouse = "%s"
                        temp_write_location = "%s"
                        """.formatted(tempDir.resolve("warehouse").toString().replace("\\", "\\\\"),
                        tempDir.resolve("tmp").toString().replace("\\", "\\\\")))
                .withFallback(ConfigFactory.load().getConfig("dazzleduck_server"))
                .resolve();
        try (var allocator = new RootAllocator()) {
            var producer = FlightSqlProducerFactory.builder(config)
                    .withAccessMode(mode)
                    .withAllocator(allocator)
                    .build();
            try {
                assertEquals(new CursorConfig(123456, 7, 99), producer.getCursorConfig(), mode.name());
            } finally {
                producer.close();
            }
        }
    }
}
