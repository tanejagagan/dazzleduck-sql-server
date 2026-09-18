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
    void dirSizeOfMissingPathIsSentinelOrZero() {
        assertEquals(-1, ResourceSampler.dirSizeBytes(null));
        assertEquals(-1, ResourceSampler.dirSizeBytes("  "));
        assertEquals(0, ResourceSampler.dirSizeBytes("/nonexistent/path/" + System.nanoTime()));
    }
}
