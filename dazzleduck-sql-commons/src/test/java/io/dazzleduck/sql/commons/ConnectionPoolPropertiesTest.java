package io.dazzleduck.sql.commons;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionPoolPropertiesTest {

    @Test
    void propertiesSetBeforeTheFirstConnectionReachDuckDb() throws Exception {
        // The instance is per class, and other tests in this JVM have likely created it already, so
        // this runs against a fresh copy of ConnectionPool that has not.
        Class<?> pool = new FreshConnectionPoolLoader().loadClass(ConnectionPool.class.getName());
        pool.getMethod("setProperty", String.class, String.class).invoke(null, "memory_limit", "1234MiB");
        pool.getMethod("setProperties", Map.class).invoke(null, Map.of("threads", "3"));
        try (Connection conn = (Connection) pool.getMethod("getConnection").invoke(null);
             ResultSet rs = conn.createStatement().executeQuery(
                     "SELECT current_setting('memory_limit'), current_setting('threads')")) {
            assertTrue(rs.next());
            assertEquals("1.2 GiB", rs.getString(1));
            assertEquals(3, rs.getInt(2));
        }
    }

    @Test
    void propertiesAfterTheInstanceExistsAreRefused() throws Exception {
        // the instance is created on the first connection; a later property would silently do nothing
        try (var conn = ConnectionPool.getConnection()) {
            assertNotNull(conn);
        }
        assertThrows(IllegalStateException.class, () -> ConnectionPool.setProperty("threads", "2"));
        assertThrows(IllegalStateException.class,
                () -> ConnectionPool.setProperties(Map.of("threads", "2", "memory_limit", "1GiB")));
    }

    @Test
    void badPropertiesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperty(" ", "x"));
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperty("threads", null));
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperties(null));
        Map<String, String> withNull = new HashMap<>();
        withNull.put("threads", null);
        assertThrows(IllegalArgumentException.class, () -> ConnectionPool.setProperties(withNull));
    }

    /** Defines ConnectionPool and its nested classes itself; everything else comes from the parent. */
    private static final class FreshConnectionPoolLoader extends ClassLoader {
        private static final String NAME = ConnectionPool.class.getName();

        FreshConnectionPoolLoader() {
            super(ConnectionPoolPropertiesTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(NAME) && !name.startsWith(NAME + "$")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        if (in == null) throw new ClassNotFoundException(name);
                        byte[] bytes = in.readAllBytes();
                        c = defineClass(name, bytes, 0, bytes.length);
                    } catch (IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                if (resolve) resolveClass(c);
                return c;
            }
        }
    }
}
