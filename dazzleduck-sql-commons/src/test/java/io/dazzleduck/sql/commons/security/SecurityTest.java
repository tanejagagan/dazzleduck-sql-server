package io.dazzleduck.sql.commons.security;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SecurityTest {

    public static String[] sqlToFail = {
            "SELECT getenv('HOME')"
    };

    public static String[] getSqlToFail() {
        return sqlToFail;
    }

    @ParameterizedTest
    @MethodSource("getSqlToFail")
    public void testFailedSql(String sql) {
        Assertions.assertThrows(Exception.class, () ->ConnectionPool.execute(sql));
    }
}
