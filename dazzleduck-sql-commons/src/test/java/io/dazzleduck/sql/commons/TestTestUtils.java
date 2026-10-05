package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTestUtils {
    @Test
    public void testIsEqualReader() throws IOException, SQLException {
        String sql = "select * from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql , 100)) {
            TestUtils.isEqual(sql, allocator, reader);
        }
    }

    @Test
    public void testIsEqual() throws SQLException, IOException {
        String sql = "select * from generate_series(10)";
        TestUtils.isEqual(sql, sql);
    }

    @Test
    public void isEqualIgnoresRowOrder() throws SQLException, IOException {
        TestUtils.isEqual("SELECT * FROM (VALUES (1), (2), (2))", "SELECT * FROM (VALUES (2), (1), (2))");
    }

    @Test
    public void isEqualSeesARepeatedRow() {
        // The case a set comparison misses: the same distinct rows, a different count of one. This
        // is how a rewrite that multiplies or collapses rows fails, so it has to be visible.
        AssertionError surplus = assertThrows(AssertionError.class,
                () -> TestUtils.isEqual("SELECT * FROM (VALUES (5), (5), (5))", "SELECT * FROM (VALUES (5))"));
        assertTrue(surplus.getMessage().contains("L->"), surplus.getMessage());
        AssertionError missing = assertThrows(AssertionError.class,
                () -> TestUtils.isEqual("SELECT * FROM (VALUES (5))", "SELECT * FROM (VALUES (5), (5))"));
        assertTrue(missing.getMessage().contains("R->"), missing.getMessage());
    }

    @Test
    public void isEqualSeesSameRowsWithDifferentMultiplicities() {
        // Same distinct rows, same total count — only the multiplicities differ.
        assertThrows(AssertionError.class,
                () -> TestUtils.isEqual("SELECT * FROM (VALUES (1), (1), (2))", "SELECT * FROM (VALUES (1), (2), (2))"));
    }
}

