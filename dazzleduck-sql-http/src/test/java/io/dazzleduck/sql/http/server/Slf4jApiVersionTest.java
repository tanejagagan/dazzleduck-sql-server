package io.dazzleduck.sql.http.server;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The flight code this module runs logs with the SLF4J 2 fluent API. jinjava depends on
 * slf4j-api 1.7, which Maven picked here until the parent pom pinned slf4j-api; on 1.x every
 * {@code logger.atDebug()} throws NoSuchMethodError (it left HTTP cancel responses hanging).
 */
class Slf4jApiVersionTest {

    @Test
    void theFluentLoggingApiIsAvailable() {
        assertDoesNotThrow(() -> LoggerFactory.getLogger(Slf4jApiVersionTest.class).atDebug().log("fluent API present"));
    }
}
