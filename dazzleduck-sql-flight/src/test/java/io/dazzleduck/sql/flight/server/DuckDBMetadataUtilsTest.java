package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DuckDBMetadataUtilsTest {

    @Test
    void getTableTypes() throws SQLException {
        int tableCount = 3;
        int count = 0;
        try(var connection = ConnectionPool.getConnection();
            var rs = DuckDBDatabaseMetadataUtil.getTableTypes(connection)) {

            while (rs.next()) {
                count++;
            }
        }
        assertEquals(count, tableCount);
    }
}
