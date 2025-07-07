package io.dazzleduck.sql.commons;

import io.dazzleduck.sql.commons.util.TestUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

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
}

