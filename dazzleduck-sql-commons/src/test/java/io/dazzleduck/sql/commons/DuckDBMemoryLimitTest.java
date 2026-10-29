package io.dazzleduck.sql.commons;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class DuckDBMemoryLimitTest {

    private static final long MIB = 1L << 20;

    @AfterEach
    void clearSystemProperties() {
        System.clearProperty(DuckDBMemoryLimit.AUTO_PROPERTY);
        System.clearProperty(DuckDBMemoryLimit.RESERVE_PROPERTY);
    }

    @Test
    void budgetIsTotalMinusHeapMinusReserve() {
        // the 2 GiB pod with a 512 MiB heap: 2048 - 512 - 512
        assertEquals(1024, DuckDBMemoryLimit.computeLimitMib(2048 * MIB, 512 * MIB, 512));
        // a bigger heap shrinks DuckDB's share by the same amount
        assertEquals(512, DuckDBMemoryLimit.computeLimitMib(2048 * MIB, 1024 * MIB, 512));
    }

    @Test
    void undersizedProcessIsFlooredNotNegative() {
        assertEquals(DuckDBMemoryLimit.MIN_LIMIT_MIB,
                DuckDBMemoryLimit.computeLimitMib(1024 * MIB, 900 * MIB, 512));
    }

    @Test
    void configuredLimitWins() {
        Properties p = new Properties();
        p.setProperty("memory_limit", "3GB");
        DuckDBMemoryLimit.applyDefault(p);
        assertEquals("3GB", p.getProperty("memory_limit"));
    }

    @Test
    void absentLimitIsComputedFromThisJvm() {
        Properties p = new Properties();
        DuckDBMemoryLimit.applyDefault(p);
        String value = p.getProperty("memory_limit");
        assertNotNull(value);
        assertTrue(value.matches("[0-9]+MiB"), value);
        long mib = Long.parseLong(value.replace("MiB", ""));
        long total = ((com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()).getTotalMemorySize();
        assertEquals(DuckDBMemoryLimit.computeLimitMib(total, Runtime.getRuntime().maxMemory(),
                DuckDBMemoryLimit.DEFAULT_RESERVE_MIB), mib);
    }

    @Test
    void reserveIsConfigurable() {
        Properties defaults = new Properties();
        DuckDBMemoryLimit.applyDefault(defaults);
        System.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "1024");
        Properties larger = new Properties();
        DuckDBMemoryLimit.applyDefault(larger);
        long a = Long.parseLong(defaults.getProperty("memory_limit").replace("MiB", ""));
        long b = Long.parseLong(larger.getProperty("memory_limit").replace("MiB", ""));
        assertTrue(b == Math.max(a - 512, DuckDBMemoryLimit.MIN_LIMIT_MIB), a + " -> " + b);
    }

    @Test
    void canBeTurnedOff() {
        System.setProperty(DuckDBMemoryLimit.AUTO_PROPERTY, "false");
        Properties p = new Properties();
        DuckDBMemoryLimit.applyDefault(p);
        assertNull(p.getProperty("memory_limit"));
    }

    @Test
    void duckDbAcceptsTheComputedValue() throws Exception {
        // the property must be a value DuckDB takes at instance creation, in MiB as written
        Properties p = new Properties();
        p.setProperty("memory_limit", "1024MiB");
        try (var conn = DriverManager.getConnection("jdbc:duckdb:", p);
             ResultSet rs = conn.createStatement().executeQuery("SELECT current_setting('memory_limit')")) {
            assertTrue(rs.next());
            assertEquals("1.0 GiB", rs.getString(1));
        }
    }
}
