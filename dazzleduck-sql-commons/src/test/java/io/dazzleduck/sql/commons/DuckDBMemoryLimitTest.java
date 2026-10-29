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

    @Test
    void propertiesWinOverSystemPropertiesAndAreNotPassedToDuckDb() {
        // the reserve and the switch as properties (ConnectionPool.setProperty / duckdb.properties)
        System.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "1024");
        System.setProperty(DuckDBMemoryLimit.AUTO_PROPERTY, "false");
        Properties p = new Properties();
        p.setProperty(DuckDBMemoryLimit.AUTO_PROPERTY, "true");
        p.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "0");
        DuckDBMemoryLimit.applyDefault(p);
        long mib = Long.parseLong(p.getProperty("memory_limit").replace("MiB", ""));
        long total = ((com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean()).getTotalMemorySize();
        assertEquals(DuckDBMemoryLimit.computeLimitMib(total, Runtime.getRuntime().maxMemory(), 0), mib);
        // DuckDB rejects an unknown option, so DazzleDuck's own keys are removed
        assertFalse(p.containsKey(DuckDBMemoryLimit.AUTO_PROPERTY));
        assertFalse(p.containsKey(DuckDBMemoryLimit.RESERVE_PROPERTY));

        Properties off = new Properties();
        off.setProperty(DuckDBMemoryLimit.AUTO_PROPERTY, "false");
        System.clearProperty(DuckDBMemoryLimit.AUTO_PROPERTY);
        DuckDBMemoryLimit.applyDefault(off);
        assertNull(off.getProperty("memory_limit"));
    }

    @Test
    void badReserveIsRejectedNotIgnored() {
        Properties negative = new Properties();
        negative.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "-5");
        assertThrows(IllegalArgumentException.class, () -> DuckDBMemoryLimit.applyDefault(negative));
        System.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "lots");
        assertThrows(IllegalArgumentException.class, () -> DuckDBMemoryLimit.applyDefault(new Properties()));
    }

    @Test
    void propertiesAfterTheInstanceExistsAreRefused() {
        // the instance is created on the first connection; a later property would silently do nothing
        try (var conn = ConnectionPool.getConnection()) {
            assertNotNull(conn);
        } catch (java.sql.SQLException e) {
            throw new RuntimeException(e);
        }
        assertThrows(IllegalStateException.class, () -> ConnectionPool.setProperty("threads", "2"));
        assertThrows(IllegalStateException.class,
                () -> ConnectionPool.setProperty(DuckDBMemoryLimit.RESERVE_PROPERTY, "256"));
        assertThrows(IllegalStateException.class,
                () -> ConnectionPool.setProperties(java.util.Map.of("threads", "2", "memory_limit", "1GiB")));
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperty(" ", "x"));
        java.util.Map<String, String> withNull = new java.util.HashMap<>();
        withNull.put("threads", null);
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperties(withNull));
    }
}
