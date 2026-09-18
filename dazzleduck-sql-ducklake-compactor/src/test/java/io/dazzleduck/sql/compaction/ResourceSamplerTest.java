package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResourceSamplerTest {

    @Test
    void parsesSizeUnits() {
        assertEquals(2L * 1024 * 1024 * 1024, ResourceSampler.parseSize("2GB"));
        assertEquals(512L * 1024 * 1024, ResourceSampler.parseSize("512 MB"));
        assertEquals(1024L, ResourceSampler.parseSize("1KB"));
        assertEquals(1073741824L, ResourceSampler.parseSize("1073741824"));
        assertEquals(-1, ResourceSampler.parseSize("notasize"));
    }

    @Test
    void extractsMemoryLimitFromConnectionSettings() {
        assertEquals(2L * 1024 * 1024 * 1024,
                ResourceSampler.memoryLimitBytes(List.of("SET threads=4", "SET memory_limit='2GB'")));
        assertEquals(-1, ResourceSampler.memoryLimitBytes(List.of("SET threads=4")));
    }

    @Test
    void extractsTempDirectoryFromConnectionSettings() {
        assertEquals("/tmp/spill",
                ResourceSampler.tempDirectory(List.of("SET temp_directory='/tmp/spill'")));
        assertNull(ResourceSampler.tempDirectory(List.of("SET memory_limit='1GB'")));
    }

    @Test
    void parsesIdleInTransactionTimeoutFromConnectionSettings() {
        assertEquals(600_000, ResourceSampler.idleInTransactionTimeoutMs(
                List.of("ATTACH 'pg' AS cat (TYPE ducklake, options='-c idle_in_transaction_session_timeout=600000')")));
        assertEquals(600_000, ResourceSampler.idleInTransactionTimeoutMs(
                List.of("SET idle_in_transaction_session_timeout='10min'")));
        assertEquals(-1, ResourceSampler.idleInTransactionTimeoutMs(List.of("SET memory_limit='2GB'")));
    }

    @Test
    void parsesDurationUnits() {
        assertEquals(600_000, ResourceSampler.parseDurationMs("600000"));
        assertEquals(600_000, ResourceSampler.parseDurationMs("600000ms"));
        assertEquals(30_000, ResourceSampler.parseDurationMs("30s"));
        assertEquals(600_000, ResourceSampler.parseDurationMs("10min"));
        assertEquals(3_600_000, ResourceSampler.parseDurationMs("1h"));
    }

    @Test
    void dirSizeOfMissingPathIsSentinelOrZero() {
        assertEquals(-1, ResourceSampler.dirSizeBytes(null));
        assertEquals(-1, ResourceSampler.dirSizeBytes("  "));
        assertEquals(0, ResourceSampler.dirSizeBytes("/nonexistent/path/" + System.nanoTime()));
    }
}
